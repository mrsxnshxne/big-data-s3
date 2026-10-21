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
ACTIVITY_WINDOWS = 8640  # 10 s windows: one day
ACTIVITY_RAW_WINDOWS = 360  # the last hour is sent as is, older windows are rolled up per minute
TOP_SESSIONS = 40
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
        self.by_tool_errors: Counter = Counter()
        self.by_status: Counter = Counter()
        self.by_agent: dict[str, Counter] = defaultdict(Counter)
        self.by_session: dict[str, Counter] = defaultdict(Counter)
        self.last_ts: dict[str, float] = {}  # session_id -> latest event timestamp
        self.total: Counter = Counter()
        self.subscribers: set[asyncio.Queue] = set()
        self.ready = False
        self.dirty = False
        self.last_event_at: float | None = None

    # ------------------------------------------------------------ state

    def _count_message(self, event: dict, sign: int) -> None:
        if event.get("role") != "assistant":
            return
        targets = (self.total, self.by_model[event.get("model") or "inconnu"],
                   self.by_agent[event.get("agent") or "inconnu"], self.by_session[event.get("session_id") or "?"])
        for target in targets:
            target["messages"] += sign
            target["cost"] += sign * (event.get("cost") or 0)
            for field in ("tokens_input", "tokens_output", "tokens_cache_read", "tokens_cache_write"):
                target[field] += sign * (event.get(field) or 0)

    def _count_tool(self, event: dict, sign: int) -> None:
        tool = event.get("tool")
        if not tool:
            return
        session = self.by_session[event.get("session_id") or "?"]
        self.by_tool[tool] += sign
        session["tools"] += sign
        if event.get("status"):
            self.by_status[event["status"]] += sign
        if event.get("status") == "error":
            self.by_tool_errors[tool] += sign
            session["errors"] += sign

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
            if key in self.tools:
                self._count_tool(self.tools[key], -1)
            self.tools[key] = event
            self._count_tool(event, +1)
        elif kind == "session":
            self.sessions[key] = event
        else:
            return
        ts, sid = parse_ts(event.get("ts")), event.get("session_id")
        if ts and sid and ts > self.last_ts.get(sid, 0):
            self.last_ts[sid] = ts
        self.dirty = True
        self.last_event_at = time.time()

    def apply_activity(self, window: dict) -> None:
        start = window.get("window_start")
        if not start:
            return
        merged = self.activity.setdefault(start, {"window_start": start, "models": {}})
        # A window re-emitted by Arroyo replaces the previous value of its model instead of adding to it.
        merged["models"][window.get("model") or "inconnu"] = {
            field: window.get(field) or 0 for field in ("messages", "cost", "tokens_input", "tokens_output")
        }
        for field in ("messages", "cost", "tokens_input", "tokens_output"):
            merged[field] = sum(m[field] for m in merged["models"].values())
        while len(self.activity) > ACTIVITY_WINDOWS:
            del self.activity[min(self.activity)]
        self.dirty = True

    # ------------------------------------------------------------ views

    def activity_view(self) -> list[dict]:
        keys = sorted(self.activity)
        old, recent = keys[:-ACTIVITY_RAW_WINDOWS], keys[-ACTIVITY_RAW_WINDOWS:]
        minutes: dict[str, dict] = {}
        for key in old:
            window = self.activity[key]
            bucket = minutes.setdefault(key[:16], {"window_start": key[:16] + ":00", "models": {}})
            for model, values in window["models"].items():
                acc = bucket["models"].setdefault(model, dict.fromkeys(values, 0))
                for field, value in values.items():
                    acc[field] += value
        for bucket in minutes.values():
            for field in ("messages", "cost", "tokens_input", "tokens_output"):
                bucket[field] = sum(m[field] for m in bucket["models"].values())
        return [*minutes.values(), *(self.activity[k] for k in recent)]

    def session_rows(self) -> list[dict]:
        rows = []
        for sid, counter in self.by_session.items():
            info = self.sessions.get(sid, {})
            rows.append({
                "id": sid, "title": info.get("title"), "agent": info.get("agent"), "model": info.get("model"),
                "last": max(self.last_ts.get(sid, 0), parse_ts(info.get("ts")) or 0),
                "messages": counter["messages"], "tools": counter["tools"], "errors": counter["errors"],
                "cost": counter["cost"], "tokens": counter["tokens_input"] + counter["tokens_output"],
                "cache_read": counter["tokens_cache_read"],
                "context": counter["tokens_input"] + counter["tokens_cache_read"] + counter["tokens_cache_write"],
            })
        rows.sort(key=lambda r: -r["last"])
        return rows[:TOP_SESSIONS]

    def stats(self) -> dict:
        now = time.time()
        active = sum(
            1 for s in self.sessions.values()
            if (ts := parse_ts(s.get("ts"))) and now - ts <= ACTIVE_SESSION_SECONDS
        )
        return {
            "now": now,
            "total": {**self.total, "tool_calls": sum(self.by_tool.values()), "tool_errors": sum(self.by_tool_errors.values()),
                      "sessions": len(self.sessions), "active_sessions": active},
            "models": {m: dict(c) for m, c in sorted(self.by_model.items(), key=lambda i: -i[1]["cost"])},
            "agents": {a: dict(c) for a, c in sorted(self.by_agent.items(), key=lambda i: -i[1]["messages"])},
            "tools": {t: {"calls": n, "errors": self.by_tool_errors[t]} for t, n in self.by_tool.most_common(15)},
            "status": {k: v for k, v in self.by_status.items() if v > 0},
            "sessions": self.session_rows(),
            "activity": self.activity_view(),
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
