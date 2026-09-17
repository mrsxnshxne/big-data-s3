"""Extract the OpenCode SQLite store into analytical Parquet datasets."""

from __future__ import annotations

import argparse
import json
import os
import sqlite3
import tempfile
from pathlib import Path
from typing import Any

import boto3
import duckdb
import pandas as pd


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


def extract(database: Path) -> dict[str, pd.DataFrame]:
    connection = sqlite3.connect(f"file:{database}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    try:
        sessions = [dict(row) for row in connection.execute("select * from session")]
        messages = [dict(row) for row in connection.execute("select * from message")]
        parts = [dict(row) for row in connection.execute("select * from part")]
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
            "created_at": pd.to_datetime(row["time_created"], unit="ms", utc=True),
            "updated_at": pd.to_datetime(row["time_updated"], unit="ms", utc=True),
        })

    message_rows = []
    for row in messages:
        data = obj(row["data"])
        message_rows.append({
            "message_id": row["id"], "session_id": row["session_id"],
            "role": data.get("role", "unknown"), "agent": data.get("agent", ""),
            "model": data.get("modelID", ""), "finish": data.get("finish", ""),
            "cost": data.get("cost", 0) or 0,
            "tokens_input": obj(data.get("tokens")).get("input", 0) or 0,
            "tokens_output": obj(data.get("tokens")).get("output", 0) or 0,
            "tokens_reasoning": obj(data.get("tokens")).get("reasoning", 0) or 0,
            "created_at": pd.to_datetime(row["time_created"], unit="ms", utc=True),
        })

    part_rows = []
    tool_rows = []
    for row in parts:
        data = obj(row["data"])
        kind = data.get("type", "unknown")
        content = text(data.get("text", data.get("content", "")))
        part_rows.append({
            "part_id": row["id"], "message_id": row["message_id"], "session_id": row["session_id"],
            "type": kind, "text": content, "created_at": pd.to_datetime(row["time_created"], unit="ms", utc=True),
        })
        if kind == "tool":
            state = obj(data.get("state"))
            tool_rows.append({
                "part_id": row["id"], "message_id": row["message_id"], "session_id": row["session_id"],
                "tool": data.get("tool", "unknown"), "call_id": data.get("callID", ""),
                "status": state.get("status", ""), "input": json.dumps(state.get("input", {}), ensure_ascii=False),
                "output": text(state.get("output", state.get("metadata", {}).get("output", ""))),
                "started_at": pd.to_datetime(obj(state.get("time")).get("start", row["time_created"]), unit="ms", utc=True),
                "created_at": pd.to_datetime(row["time_created"], unit="ms", utc=True),
            })

    return {
        "sessions": pd.DataFrame(session_rows), "messages": pd.DataFrame(message_rows),
        "parts": pd.DataFrame(part_rows), "tools": pd.DataFrame(tool_rows),
    }


def write_parquet(datasets: dict[str, pd.DataFrame], output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    columns = {
        "sessions": ["session_id", "title", "directory", "agent", "model", "provider", "cost", "tokens_input", "tokens_output", "tokens_reasoning", "tokens_cache_read", "tokens_cache_write", "created_at", "updated_at"],
        "messages": ["message_id", "session_id", "role", "agent", "model", "finish", "cost", "tokens_input", "tokens_output", "tokens_reasoning", "created_at"],
        "parts": ["part_id", "message_id", "session_id", "type", "text", "created_at"],
        "tools": ["part_id", "message_id", "session_id", "tool", "call_id", "status", "input", "output", "started_at", "created_at"],
    }
    for name, frame in datasets.items():
        if frame.empty:
            frame = pd.DataFrame(columns=columns[name])
        frame.to_parquet(output / f"{name}.parquet", index=False)


def upload(output: Path) -> None:
    client = boto3.client(
        "s3", endpoint_url=os.getenv("S3_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "rustfsadmin"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "rustfsadmin"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )
    bucket = os.getenv("S3_BUCKET", "opencode-analytics")
    try:
        client.head_bucket(Bucket=bucket)
    except client.exceptions.ClientError:
        client.create_bucket(Bucket=bucket)
    prefix = os.getenv("S3_PREFIX", "parquet")
    for path in output.glob("*.parquet"):
        client.upload_file(str(path), bucket, f"{prefix}/{path.name}")
        print(f"uploaded s3://{bucket}/{prefix}/{path.name}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", type=Path, default=Path.home() / ".local/share/opencode/opencode.db")
    parser.add_argument("--output", type=Path, default=Path("data/parquet"))
    parser.add_argument("--upload", action="store_true", help="upload Parquet files to RustFS")
    args = parser.parse_args()
    if not args.database.exists():
        parser.error(f"OpenCode database not found: {args.database}")
    datasets = extract(args.database)
    write_parquet(datasets, args.output)
    print(f"extracted {len(datasets['sessions'])} sessions, {len(datasets['messages'])} messages, {len(datasets['parts'])} parts")
    if args.upload:
        upload(args.output)


if __name__ == "__main__":
    main()
