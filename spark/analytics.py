"""Build OpenCode activity aggregates with Apache Spark.

Source tables come from the Iceberg catalog maintained by the streaming sink.
For local development without a catalog, set ``SPARK_INPUT`` to a directory of
Parquet files instead.
"""

from __future__ import annotations

import argparse
import os

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


TABLES = ("sessions", "messages", "tools")
ICEBERG_CATALOG = os.getenv("SPARK_ICEBERG_CATALOG", "lakehouse")
ICEBERG_NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "analytics")


def spark_session(master: str | None) -> SparkSession:
    builder = SparkSession.builder.appName("OpenCodeAnalytics")
    if master:
        builder = builder.master(master)

    endpoint = os.getenv("S3_ENDPOINT") or os.getenv("ICEBERG_S3_ENDPOINT")
    if endpoint:
        builder = (
            builder.config("spark.hadoop.fs.s3a.endpoint", endpoint)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", endpoint.startswith("https://"))
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("S3_ACCESS_KEY", "rustfsadmin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("S3_SECRET_KEY", "rustfsadmin"))
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
        )

    catalog_uri = os.getenv("ICEBERG_CATALOG_URI")
    if catalog_uri:
        prefix = f"spark.sql.catalog.{ICEBERG_CATALOG}"
        builder = (
            builder.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config(prefix, "org.apache.iceberg.spark.SparkCatalog")
            .config(f"{prefix}.catalog-impl", "org.apache.iceberg.rest.RESTCatalog")
            .config(f"{prefix}.uri", catalog_uri)
            .config(f"{prefix}.warehouse", os.getenv("ICEBERG_WAREHOUSE", "s3://iceberg-warehouse"))
            .config(f"{prefix}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
            .config(f"{prefix}.client.region", os.getenv("AWS_DEFAULT_REGION", "us-east-1"))
            .config(f"{prefix}.s3.access-key-id", os.getenv("S3_ACCESS_KEY", "rustfsadmin"))
            .config(f"{prefix}.s3.secret-access-key", os.getenv("S3_SECRET_KEY", "rustfsadmin"))
            .config(
                f"{prefix}.s3.endpoint",
                os.getenv("ICEBERG_S3_ENDPOINT", os.getenv("S3_ENDPOINT", "http://localhost:9000")),
            )
            .config(
                f"{prefix}.s3.path-style-access",
                os.getenv("ICEBERG_S3_PATH_STYLE_ACCESS", "true"),
            )
        )
    return builder.getOrCreate()


def read_tables(spark: SparkSession, input_path: str | None) -> dict[str, DataFrame]:
    catalog_uri = os.getenv("ICEBERG_CATALOG_URI")
    keys = {"sessions": "session_id", "messages": "message_id", "tools": "part_id"}
    tables = {}
    for name in TABLES:
        if catalog_uri:
            tables[name] = deduplicate(spark.table(f"{ICEBERG_CATALOG}.{ICEBERG_NAMESPACE}.{name}"), keys[name])
        else:
            if not input_path:
                raise RuntimeError("Set ICEBERG_CATALOG_URI to read Iceberg, or SPARK_INPUT to read Parquet")
            tables[name] = spark.read.parquet(f"{input_path.rstrip('/')}/{name}.parquet")
    return tables


def deduplicate(frame: DataFrame, key: str) -> DataFrame:
    """The sink appends events; keep the latest version of every row."""
    window = Window.partitionBy(key).orderBy(F.col("updated_at").desc())
    return (
        frame.withColumn("_version", F.row_number().over(window))
        .filter(F.col("_version") == 1)
        .drop("_version")
    )


def build_metrics(tables: dict[str, DataFrame]) -> dict[str, DataFrame]:
    sessions = tables["sessions"]
    messages = tables["messages"]
    tools = tables["tools"]

    daily_activity = (
        messages.withColumn("activity_date", F.to_date("created_at"))
        .groupBy("activity_date")
        .agg(
            F.count("message_id").alias("message_count"),
            F.countDistinct("session_id").alias("session_count"),
            F.sum("tokens_input").alias("tokens_input"),
            F.sum("tokens_output").alias("tokens_output"),
            F.sum("tokens_reasoning").alias("tokens_reasoning"),
            F.sum("cost").alias("cost"),
        )
        .orderBy("activity_date")
    )

    session_summary = (
        sessions.join(
            messages.groupBy("session_id").agg(
                F.count("message_id").alias("message_count"),
                F.sum("tokens_input").alias("message_tokens_input"),
                F.sum("tokens_output").alias("message_tokens_output"),
            ),
            "session_id",
            "left",
        )
        .join(
            tools.groupBy("session_id").agg(F.count("part_id").alias("tool_call_count")),
            "session_id",
            "left",
        )
        .fillna(0, subset=["message_count", "message_tokens_input", "message_tokens_output", "tool_call_count"])
        .withColumn("duration_minutes", (F.col("updated_at").cast("long") - F.col("created_at").cast("long")) / 60)
        .select(
            "session_id", "title", "directory", "agent", "model", "provider", "created_at", "updated_at",
            "duration_minutes", "message_count", "tool_call_count", "cost",
            "message_tokens_input", "message_tokens_output",
        )
    )

    tool_usage = (
        tools.groupBy("tool")
        .agg(
            F.count("part_id").alias("call_count"),
            F.countDistinct("session_id").alias("session_count"),
            F.sum(F.when(F.col("status") == "completed", 1).otherwise(0)).alias("completed_count"),
            F.sum(F.when(F.col("status") == "error", 1).otherwise(0)).alias("error_count"),
        )
        .orderBy(F.desc("call_count"))
    )

    model_usage = (
        messages.where(F.col("model") != "")
        .groupBy("model")
        .agg(
            F.count("message_id").alias("message_count"),
            F.countDistinct("session_id").alias("session_count"),
            F.sum("tokens_input").alias("tokens_input"),
            F.sum("tokens_output").alias("tokens_output"),
            F.sum("cost").alias("cost"),
        )
        .orderBy(F.desc("message_count"))
    )

    return {
        "daily_activity": daily_activity,
        "session_summary": session_summary,
        "tool_usage": tool_usage,
        "model_usage": model_usage,
    }


def write_metrics(metrics: dict[str, DataFrame], output_path: str) -> None:
    output = output_path.rstrip("/")
    for name, frame in metrics.items():
        writer = frame.write.mode("overwrite")
        if name == "daily_activity":
            writer = writer.partitionBy("activity_date")
        writer.parquet(f"{output}/{name}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", default=os.getenv("SPARK_INPUT"), help="Parquet fallback when no Iceberg catalog is configured")
    parser.add_argument("--output", default=os.getenv("SPARK_OUTPUT", "data/spark"))
    parser.add_argument("--master", default=os.getenv("SPARK_MASTER", "local[*]"))
    args = parser.parse_args()

    spark = spark_session(args.master)
    try:
        metrics = build_metrics(read_tables(spark, args.input))
        write_metrics(metrics, args.output)
        for name, frame in metrics.items():
            print(f"wrote {name}: {frame.count()} rows")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
