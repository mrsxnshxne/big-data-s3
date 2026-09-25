#!/usr/bin/env sh
set -eu

PACKAGES="org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION:-3.3.4}"
if [ -n "${ICEBERG_CATALOG_URI:-}" ]; then
  PACKAGES="${PACKAGES},org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:${ICEBERG_SPARK_RUNTIME:-1.6.1}"
fi

exec spark-submit \
  --packages "$PACKAGES" \
  spark/analytics.py "$@"
