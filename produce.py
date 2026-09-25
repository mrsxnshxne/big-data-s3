"""Publish OpenCode SQLite activity to Redpanda topics as incremental events.

The producer reads the local OpenCode database in read-only mode, keeps a
watermark per dataset (last ``time_updated`` in epoch milliseconds) and sends
every new or modified row as a JSON event to a Redpanda (Kafka-compatible)
topic. A separate streaming sink (``sink.py``) appends those events to the
Iceberg tables consumed by the dashboard.
"""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
from pathlib import Path
from typing import Any

import pandas as pd


DATASETS = ("sessions", "messages", "parts", "tools")

COLUMNS: dict[str, list[str]] = {
    "sessions": ["session_id", "title", "directory", "agent", "model", "provider", "cost", "tokens_input", "tokens_output", "tokens_reasoning", "tokens_cache_read", "tokens_cache_write", "created_at", "updated_at"],
    "messages": ["message_id", "session_id", "role", "agent", "model", "finish", "cost", "tokens_input", "tokens_output", "tokens_reasoning", "created_at", "updated_at"],
    "parts": ["part_id", "message_id", "session_id", "type", "text", "created_at", "updated_at"],
    "tools": ["part_id", "message_id", "session_id", "tool", "call_id", "status", "input", "output", "started_at", "created_at", "updated_at"],
}

KEYS = {"sessions": "session_id", "messages": "message_id", "parts": "part_id", "tools": "part_id"}


def obj(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    try:
        parsed = json.loads(value or "{}")
        return parsed if isinstance(parsed, dict) else {}
    except (TypeError, json.JSONDecodeError):
        return {}


def text(value: Any) -> str:
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "\n".join(text(item) for item in value)
    if isinstance(value, dict):
        return str(value.get("text", value.get("content", "")))
    return "" if value is None else str(value)


def timestamp(value: Any) -> pd.Timestamp | None:
    if value is None:
        return None
    return pd.to_datetime(value, unit="ms", utc=True)


def extract(database: Path, watermarks: dict[str, int]) -> dict[str, pd.DataFrame]:
    """Read every row updated after the stored watermark for each dataset."""
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        sessions = [dict(row) for row in connection.execute(
            "select * from session where time_updated > ? order by time_updated", (watermarks.get("sessions", 0),))]
        messages = [dict(row) for row in connection.execute(
            "select * from message where time_updated > ? order by time_updated", (watermarks.get("messages", 0),))]
        parts = [dict(row) for row in connection.execute(
            "select * from part where time_updated > ? order by time_updated", (watermarks.get("parts", 0),))]
    finally:
        connection.close()

    session_rows = []
    for row in sessions:
        model = obj(row.get("model"))
        session_rows.append({
            "session_id": row["id"], "title": row["title"], "directory": row["directory"],
            "agent": row["agent"] or "", "model": model.get("id", row.get("model") or ""),
            "provider": model.get("providerID", ""), "cost": row["cost"] or 0,
            "tokens_input": row["tokens_input"] or 0, "tokens_output": row["tokens_output"] or 0,
            "tokens_reasoning": row["tokens_reasoning"] or 0,
            "tokens_cache_read": row["tokens_cache_read"] or 0,
            "tokens_cache_write": row["tokens_cache_write"] or 0,
            "created_at": timestamp(row["time_created"]),
            "updated_at": timestamp(row["time_updated"]),
            "_watermark": row["time_updated"],
        })

    message_rows = []
    for row in messages:
        data = obj(row["data"])
        tokens = obj(data.get("tokens"))
        message_rows.append({
            "message_id": row["id"], "session_id": row["session_id"],
            "role": data.get("role", "unknown"), "agent": data.get("agent", ""),
            "model": data.get("modelID", ""), "finish": data.get("finish", ""),
            "cost": data.get("cost", 0) or 0,
            "tokens_input": tokens.get("input", 0) or 0,
            "tokens_output": tokens.get("output", 0) or 0,
            "tokens_reasoning": tokens.get("reasoning", 0) or 0,
            "created_at": timestamp(row["time_created"]),
            "updated_at": timestamp(row["time_updated"]),
            "_watermark": row["time_updated"],
        })

    part_rows = []
    tool_rows = []
    for row in parts:
        data = obj(row["data"])
        kind = data.get("type", "unknown")
        content = text(data.get("text", data.get("content", "")))
        part_rows.append({
            "part_id": row["id"], "message_id": row["message_id"], "session_id": row["session_id"],
            "type": kind, "text": content,
            "created_at": timestamp(row["time_created"]),
            "updated_at": timestamp(row["time_updated"]),
            "_watermark": row["time_updated"],
        })
        if kind == "tool":
            state = obj(data.get("state"))
            tool_rows.append({
                "part_id": row["id"], "message_id": row["message_id"], "session_id": row["session_id"],
                "tool": data.get("tool", "unknown"), "call_id": data.get("callID", ""),
                "status": state.get("status", ""), "input": json.dumps(state.get("input", {}), ensure_ascii=False),
                "output": text(state.get("output", state.get("metadata", {}).get("output", ""))),
                "started_at": timestamp(obj(state.get("time")).get("start", row["time_created"])),
                "created_at": timestamp(row["time_created"]),
                "updated_at": timestamp(row["time_updated"]),
                "_watermark": row["time_updated"],
            })

    frames = {}
    for name, rows in (("sessions", session_rows), ("messages", message_rows), ("parts", part_rows), ("tools", tool_rows)):
        frames[name] = pd.DataFrame(rows, columns=COLUMNS[name] + ["_watermark"])
    return frames


def json_safe(value: Any) -> Any:
    if isinstance(value, pd.Timestamp):
        return None if pd.isna(value) else value.isoformat()
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return value


def to_events(frame: pd.DataFrame, dataset: str) -> list[tuple[str, dict[str, Any]]]:
    events = []
    for row in frame.to_dict(orient="records"):
        record = {column: json_safe(row.get(column)) for column in COLUMNS[dataset]}
        events.append((str(row[KEYS[dataset]]), record))
    return events


def topic(name: str) -> str:
    return f"{os.getenv('KAFKA_TOPIC_PREFIX', 'opencode')}.{name}"


def produce(datasets: dict[str, pd.DataFrame]) -> dict[str, int]:
    from kafka import KafkaProducer

    servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    producer = KafkaProducer(
        bootstrap_servers=[server.strip() for server in servers.split(",")],
        acks="all",
        linger_ms=int(os.getenv("KAFKA_LINGER_MS", "50")),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
    )
    counts = {}
    try:
        for name, frame in datasets.items():
            sent = 0
            for key, record in to_events(frame, name):
                producer.send(topic(name), key=key, value=record)
                sent += 1
            counts[name] = sent
        # Raises on any failed delivery; the caller only saves the watermark after this returns.
        producer.flush(timeout=120)
    finally:
        producer.close(timeout=30)
    return counts


def load_state(path: Path) -> dict[str, int]:
    if not path.exists():
        return {}
    try:
        raw = json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return {name: int(raw.get(name, 0)) for name in DATASETS}


def save_state(path: Path, datasets: dict[str, pd.DataFrame], previous: dict[str, int]) -> None:
    state = dict(previous)
    for name, frame in datasets.items():
        if not frame.empty:
            state[name] = int(max(state.get(name, 0), int(frame["_watermark"].max())))
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=Path(os.getenv("OPENCODE_DB", Path.home() / ".local/share/opencode/opencode.db")))
    parser.add_argument("--state", type=Path, default=Path(os.getenv("PRODUCER_STATE_FILE", "data/state/producer-state.json")))
    parser.add_argument("--full", action="store_true", help="ignore watermarks and re-publish the whole history")
    parser.add_argument("--dry-run", action="store_true", help="count pending events without contacting Redpanda")
    args = parser.parse_args()
    if not args.database.exists():
        parser.error(f"OpenCode database not found: {args.database}")

    watermarks = {} if args.full else load_state(args.state)
    datasets = extract(args.database, watermarks)
    summary = ", ".join(f"{name} {len(frame)}" for name, frame in datasets.items())
    if args.dry_run:
        print(f"dry run: pending events -> {summary}")
        return

    counts = produce(datasets)
    save_state(args.state, datasets, watermarks)
    print(f"produced to {os.getenv('KAFKA_TOPIC_PREFIX', 'opencode')}.* -> " + ", ".join(f"{counts[name]} {name}" for name in DATASETS))


if __name__ == "__main__":
    main()
