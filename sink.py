"""Stream Redpanda events into Iceberg tables.

This consumer subscribes to the OpenCode topics produced by ``produce.py``,
batches the JSON events and appends them to the ``analytics`` tables of the
configured Iceberg catalog. The physical data lands in S3/RustFS because the
Iceberg warehouse points there. Offsets are committed only after a successful
append, so the pipeline is at-least-once; readers deduplicate by id.
"""

from __future__ import annotations

import json
import os
import signal
import time
from urllib.parse import urlparse

import boto3
import pandas as pd
import pyarrow as pa
from botocore.exceptions import ClientError
from kafka import KafkaConsumer
from pyiceberg.catalog import load_catalog
from pyiceberg.io.pyarrow import pyarrow_to_schema
from pyiceberg.table.name_mapping import MappedField, NameMapping


NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "analytics")

TS = pa.timestamp("us", tz="UTC")

ARROW_SCHEMAS: dict[str, pa.Schema] = {
    "sessions": pa.schema([
        pa.field("session_id", pa.string()), pa.field("title", pa.string()),
        pa.field("directory", pa.string()), pa.field("agent", pa.string()),
        pa.field("model", pa.string()), pa.field("provider", pa.string()),
        pa.field("cost", pa.float64()),
        pa.field("tokens_input", pa.int64()), pa.field("tokens_output", pa.int64()),
        pa.field("tokens_reasoning", pa.int64()), pa.field("tokens_cache_read", pa.int64()),
        pa.field("tokens_cache_write", pa.int64()),
        pa.field("created_at", TS), pa.field("updated_at", TS),
    ]),
    "messages": pa.schema([
        pa.field("message_id", pa.string()), pa.field("session_id", pa.string()),
        pa.field("role", pa.string()), pa.field("agent", pa.string()),
        pa.field("model", pa.string()), pa.field("finish", pa.string()),
        pa.field("cost", pa.float64()),
        pa.field("tokens_input", pa.int64()), pa.field("tokens_output", pa.int64()),
        pa.field("tokens_reasoning", pa.int64()),
        pa.field("created_at", TS), pa.field("updated_at", TS),
    ]),
    "parts": pa.schema([
        pa.field("part_id", pa.string()), pa.field("message_id", pa.string()),
        pa.field("session_id", pa.string()), pa.field("type", pa.string()),
        pa.field("text", pa.string()), pa.field("created_at", TS), pa.field("updated_at", TS),
    ]),
    "tools": pa.schema([
        pa.field("part_id", pa.string()), pa.field("message_id", pa.string()),
        pa.field("session_id", pa.string()), pa.field("tool", pa.string()),
        pa.field("call_id", pa.string()), pa.field("status", pa.string()),
        pa.field("input", pa.string()), pa.field("output", pa.string()),
        pa.field("started_at", TS), pa.field("created_at", TS), pa.field("updated_at", TS),
    ]),
}

TIMESTAMP_COLUMNS = {
    name: [field.name for field in schema if pa.types.is_timestamp(field.type)]
    for name, schema in ARROW_SCHEMAS.items()
}


def iceberg_schema(name: str) -> object:
    schema = ARROW_SCHEMAS[name]
    name_mapping = NameMapping([
        MappedField(field_id=index, names=[field.name])
        for index, field in enumerate(schema, start=1)
    ])
    return pyarrow_to_schema(schema, name_mapping=name_mapping)


def to_arrow(name: str, records: list[dict]) -> pa.Table:
    frame = pd.DataFrame(records, columns=[field.name for field in ARROW_SCHEMAS[name]])
    for column in TIMESTAMP_COLUMNS[name]:
        frame[column] = pd.to_datetime(frame[column], utc=True, errors="coerce")
    for field in ARROW_SCHEMAS[name]:
        if pa.types.is_integer(field.type):
            frame[field.name] = frame[field.name].astype("Int64")
        elif pa.types.is_floating(field.type):
            frame[field.name] = pd.to_numeric(frame[field.name], errors="coerce")
    return pa.Table.from_pandas(frame, schema=ARROW_SCHEMAS[name], preserve_index=False)


def ensure_warehouse_bucket() -> None:
    warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3://iceberg-warehouse")
    bucket = urlparse(warehouse).netloc
    if not bucket:
        return
    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("ICEBERG_S3_ENDPOINT", os.getenv("S3_ENDPOINT", "http://localhost:9000")),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "rustfsadmin"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "rustfsadmin"),
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    )
    try:
        client.head_bucket(Bucket=bucket)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") not in {"404", "NoSuchBucket", "NotFound"}:
            raise
        client.create_bucket(Bucket=bucket)
        print(f"created bucket s3://{bucket}", flush=True)


def load_iceberg_catalog():
    catalog_type = os.getenv("ICEBERG_CATALOG_TYPE", "rest")
    warehouse = os.getenv("ICEBERG_WAREHOUSE", "s3://iceberg-warehouse")
    if catalog_type == "sql":
        return load_catalog(
            "opencode",
            type="sql",
            uri=os.getenv("ICEBERG_SQL_URI", "sqlite:///data/state/iceberg-catalog.db"),
            warehouse=warehouse,
        )
    return load_catalog(
        "opencode",
        type="rest",
        uri=os.getenv("ICEBERG_CATALOG_URI", "http://localhost:8181"),
        warehouse=warehouse,
        **{
            "s3.endpoint": os.getenv("ICEBERG_S3_ENDPOINT", os.getenv("S3_ENDPOINT", "http://localhost:9000")),
            "s3.access-key-id": os.getenv("S3_ACCESS_KEY", "rustfsadmin"),
            "s3.secret-access-key": os.getenv("S3_SECRET_KEY", "rustfsadmin"),
            "s3.region": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
            "s3.path-style-access": os.getenv("ICEBERG_S3_PATH_STYLE_ACCESS", "true"),
        },
    )


def make_consumer() -> KafkaConsumer:
    prefix = os.getenv("KAFKA_TOPIC_PREFIX", "opencode")
    return KafkaConsumer(
        *[f"{prefix}.{name}" for name in ARROW_SCHEMAS],
        bootstrap_servers=[server.strip() for server in os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092").split(",")],
        group_id=os.getenv("KAFKA_CONSUMER_GROUP", "opencode-iceberg-sink"),
        auto_offset_reset=os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest"),
        enable_auto_commit=False,
        value_deserializer=lambda raw: json.loads(raw.decode("utf-8")),
    )


class IcebergSink:
    def __init__(self) -> None:
        ensure_warehouse_bucket()
        self.catalog = load_iceberg_catalog()
        self.catalog.create_namespace_if_not_exists(NAMESPACE)

    def append(self, name: str, records: list[dict]) -> None:
        identifier = f"{NAMESPACE}.{name}"
        table = self.catalog.create_table_if_not_exists(identifier, schema=iceberg_schema(name))
        table.append(to_arrow(name, records))
        print(f"appended {len(records)} rows to {identifier}", flush=True)


def dataset_for(topic: str) -> str:
    return topic.rsplit(".", 1)[-1]


def run(once: bool) -> None:
    consumer = make_consumer()
    sink = IcebergSink()
    max_records = int(os.getenv("SINK_MAX_RECORDS", "2000"))
    flush_interval = float(os.getenv("SINK_FLUSH_INTERVAL_SECONDS", "5"))
    idle_timeout = float(os.getenv("SINK_IDLE_TIMEOUT_SECONDS", "3"))
    stop = {"requested": False}

    def handle_signal(signum, frame):
        stop["requested"] = True

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    buffers: dict[str, list[dict]] = {}
    last_flush = time.monotonic()
    last_message = time.monotonic()
    while not stop["requested"]:
        batch = consumer.poll(timeout_ms=1000)
        now = time.monotonic()
        if batch:
            last_message = now
            for record in batch.values():
                for message in record:
                    buffers.setdefault(dataset_for(message.topic), []).append(message.value)
        pending = sum(len(rows) for rows in buffers.values())
        if pending and (pending >= max_records or now - last_flush >= flush_interval):
            for name, rows in buffers.items():
                sink.append(name, rows)
            consumer.commit()
            buffers.clear()
            last_flush = time.monotonic()
        if once and not batch and time.monotonic() - last_message >= idle_timeout:
            break
    for name, rows in buffers.items():
        sink.append(name, rows)
    consumer.commit()
    consumer.close()


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--once", action="store_true", help="flush pending events and exit when the topics go idle")
    args = parser.parse_args()
    run(once=args.once)


if __name__ == "__main__":
    main()
