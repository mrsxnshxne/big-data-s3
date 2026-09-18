#!/usr/bin/env sh
set -eu

exec spark-submit \
  --packages "org.apache.hadoop:hadoop-aws:${HADOOP_AWS_VERSION:-3.3.4}" \
  spark/analytics.py "$@"
