"""SSE bridge: Redpanda ``live.*`` topics (written by Arroyo) -> browser panel.

A consumer thread reads ``live.events`` and ``live.activity`` from the beginning
to rebuild the totals, then keeps following them. Each browser connected to
``/stream`` first receives a ``snapshot`` and then one ``event`` per new record,
plus a ``stats`` update (at most once a second) while something changed. The
panel itself (``static/index.html``) is served from ``/``.

Events carry no prompt, tool input or output: Arroyo does not forward them.
"""

from __future__ import annotations

import asyncio
import json
import os
import threading
import time
from collections import Counter, defaultdict, deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import uvicorn
from confluent_kafka import OFFSET_BEGINNING, Consumer
from fastapi import FastAPI
from fastapi.responses import FileResponse, StreamingResponse

STATIC = Path(__file__).resolve().parent / "static"
EVENTS_TOPIC = os.getenv("LIVE_EVENTS_TOPIC", "live.events")
ACTIVITY_TOPIC = os.getenv("LIVE_ACTIVITY_TOPIC", "live.activity")
RECENT_EVENTS = 200
ACTIVITY_WINDOWS = 90
ACTIVE_SESSION_SECONDS = 300
# Arroyo replays the whole history into live.events when a pipeline is (re)deployed: only recent
# events are animated in the panel, older ones just feed the totals.
LIVE_MAX_AGE_SECONDS = 600


def parse_ts(value: str | None) -> float | None:
    """Arroyo writes timestamps as ISO 8601; naive values are UTC."""
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
    return (parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)).timestamp()


class Hub:
    """Latest version of every message/tool/session, running totals and SSE subscribers."""

    def __init__(self) -> None:
        self.messages: dict[str, dict] = {}
        self.tools: dict[str, dict] = {}
        self.sessions: dict[str, dict] = {}
        self.activity: dict[str, dict] = {}  # window_start -> merged windows of every model
        self.recent: deque[dict] = deque(maxlen=RECENT_EVENTS)
        self.by_model: dict[str, Counter] = defaultdict(Counter)
        self.by_tool: Counter = Counter()
        self.total: Counter = Counter()
        self.subscribers: set[asyncio.Queue] = set()
        self.ready = False
        self.dirty = False
        self.last_event_at: float | None = None

    # ------------------------------------------------------------ state

    def _count_message(self, event: dict, sign: int) -> None:
        if event.get("role") != "assistant":
            return
        for target in (self.total, self.by_model[event.get("model") or "inconnu"]):
            target["messages"] += sign
            target["cost"] += sign * (event.get("cost") or 0)
            for field in ("tokens_input", "tokens_output", "tokens_cache_read", "tokens_cache_write"):
                target[field] += sign * (event.get(field) or 0)

    def apply(self, event: dict) -> None:
        kind, key = event.get("kind"), event.get("id")
        if not key:
            return
        if kind == "message":
            if key in self.messages:
                self._count_message(self.messages[key], -1)
            self.messages[key] = event
            self._count_message(event, +1)
        elif kind == "tool":
            previous = self.tools.get(key)
            if previous and previous.get("tool"):
                self.by_tool[previous["tool"]] -= 1
            self.tools[key] = event
            if event.get("tool"):
                self.by_tool[event["tool"]] += 1
        elif kind == "session":
            self.sessions[key] = event
        else:
            return
        self.dirty = True
        self.last_event_at = time.time()

    def apply_activity(self, window: dict) -> None:
        start = window.get("window_start")
        if not start:
            return
        merged = self.activity.setdefault(start, {"window_start": start, "messages": 0, "cost": 0.0, "models": {}})
        merged["models"][window.get("model") or "inconnu"] = window.get("messages") or 0
        merged["messages"] = sum(merged["models"].values())
        merged["cost"] = merged.get("cost", 0.0) + (window.get("cost") or 0)
        while len(self.activity) > ACTIVITY_WINDOWS:
            del self.activity[min(self.activity)]
        self.dirty = True

    # ------------------------------------------------------------ views

    def stats(self) -> dict:
        now = time.time()
        active = sum(
            1 for s in self.sessions.values()
            if (ts := parse_ts(s.get("ts"))) and now - ts <= ACTIVE_SESSION_SECONDS
        )
        return {
            "now": now,
            "total": {**self.total, "tool_calls": sum(self.by_tool.values()), "sessions": len(self.sessions),
                      "active_sessions": active},
            "models": {m: dict(c) for m, c in sorted(self.by_model.items(), key=lambda i: -i[1]["cost"])},
            "tools": dict(self.by_tool.most_common(12)),
            "activity": [self.activity[k] for k in sorted(self.activity)],
            "last_event_at": self.last_event_at,
        }

    def snapshot(self) -> dict:
        titles = {sid: s.get("title") for sid, s in self.sessions.items()}
        return {"stats": self.stats(), "recent": list(self.recent), "titles": titles}

    # ------------------------------------------------------------ fan-out

    def handle_batch(self, events: list[dict], activity: list[dict], live: list[bool], ready: bool) -> None:
        """Runs on the event loop: every state mutation happens on this single thread."""
        for event, is_live in zip(events, live):
            self.apply(event)
            if event.get("kind") == "session":
                if is_live:
                    self._publish("session", {"id": event["id"], "title": event.get("title")})
                continue
            self.recent.append(event)
            if is_live:
                self._publish("event", event)
        for window in activity:
            self.apply_activity(window)
        if ready and not self.ready:
            self.ready = True
            self.dirty = True

    def _publish(self, name: str, data: dict) -> None:
        payload = f"event: {name}\ndata: {json.dumps(data, separators=(',', ':'))}\n\n"
        for queue in list(self.subscribers):
            if queue.full():  # slow client: drop it, the browser reconnects and gets a fresh snapshot
                self.subscribers.discard(queue)
                queue.put_nowait(None)
                continue
            queue.put_nowait(payload)

    async def ticker(self) -> None:
        while True:
            await asyncio.sleep(1)
            if self.dirty and self.ready:
                self.dirty = False
                self._publish("stats", self.stats())
            else:
                self._publish_keepalive()

    def _publish_keepalive(self) -> None:
        for queue in list(self.subscribers):
            if not queue.full():
                queue.put_nowait(": keepalive\n\n")


hub = Hub()


def consume(loop: asyncio.AbstractEventLoop, stop: threading.Event) -> None:
    """Follow both topics from the beginning; messages below the start-up high watermark are backlog."""
    backlog_end: dict[tuple[str, int], int] = {}
    consumer = Consumer({
        "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
        "group.id": f"live-bridge-{os.getpid()}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })

    caught_up: set[tuple[str, int]] = set()

    def on_assign(c: Consumer, partitions: list) -> None:
        for partition in partitions:
            low, high = c.get_watermark_offsets(partition, cached=False)
            key = (partition.topic, partition.partition)
            backlog_end[key] = high
            if low >= high:  # empty partition: nothing to replay
                caught_up.add(key)
            partition.offset = OFFSET_BEGINNING
        c.assign(partitions)

    consumer.subscribe([EVENTS_TOPIC, ACTIVITY_TOPIC], on_assign=on_assign)
    while not stop.is_set():
        batch = consumer.consume(num_messages=500, timeout=0.5)
        events, live, activity = [], [], []
        for message in batch:
            if message.error():
                print(f"consumer warning: {message.error()}", flush=True)
                continue
            try:
                doc = json.loads(message.value())
            except (TypeError, ValueError):
                continue
            tp = (message.topic(), message.partition())
            ts = parse_ts(doc.get("ts"))
            is_live = message.offset() >= backlog_end.get(tp, 0) and (
                message.topic() == ACTIVITY_TOPIC or (ts is not None and time.time() - ts <= LIVE_MAX_AGE_SECONDS))
            if message.offset() + 1 >= backlog_end.get(tp, 0):
                caught_up.add(tp)
            if message.topic() == ACTIVITY_TOPIC:
                activity.append(doc)
            else:
                events.append(doc)
                live.append(is_live)
        ready = bool(backlog_end) and caught_up >= set(backlog_end)
        if events or activity or ready:
            loop.call_soon_threadsafe(hub.handle_batch, events, activity, live, ready)
    consumer.close()


@asynccontextmanager
async def lifespan(_: FastAPI):
    stop = threading.Event()
    loop = asyncio.get_running_loop()
    thread = threading.Thread(target=consume, args=(loop, stop), daemon=True)
    thread.start()
    ticker = asyncio.create_task(hub.ticker())
    yield
    stop.set()
    ticker.cancel()


app = FastAPI(lifespan=lifespan)


@app.get("/")
def index() -> FileResponse:
    return FileResponse(STATIC / "index.html")


@app.get("/health")
def health() -> dict:
    return {"ready": hub.ready, "messages": len(hub.messages), "subscribers": len(hub.subscribers)}


@app.get("/stream")
async def stream() -> StreamingResponse:
    queue: asyncio.Queue = asyncio.Queue(maxsize=1000)
    # Snapshot and subscription happen in the same loop turn: no event can slip in between.
    first = f"retry: 2000\nevent: snapshot\ndata: {json.dumps(hub.snapshot(), separators=(',', ':'))}\n\n"
    hub.subscribers.add(queue)

    async def body():
        try:
            yield first
            while (chunk := await queue.get()) is not None:
                yield chunk
        finally:
            hub.subscribers.discard(queue)

    return StreamingResponse(
        body(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("LIVE_PORT_INTERNAL", "8600")), log_level="warning")
