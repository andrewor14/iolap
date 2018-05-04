#!/bin/bash

LOG_DIR="${LOG_DIR:-/disk/local/disk2/stafman/logs}"
INPUT_DATA="${INPUT_DATA:-/disk/local/disk2/stafman/students56g.json}"
OUTPUT_DATA="${OUTPUT_DATA:-/disk/local/disk2/stafman/students56g_16k.parquet}"
EXPR_NAME="${EXPR_NAME:-json2parquet}"
RUNTIME_MEMORY="${RUNTIME_MEMORY:-45g}"

# Print all flags to log file
LOG_FILE_NAME="$LOG_DIR/$EXPR_NAME".log
# Run Spark
bin/spark-submit\
  --master local[*]\
  --driver-memory "$RUNTIME_MEMORY"\
  --executor-memory "$RUNTIME_MEMORY"\
  --class org.apache.spark.examples.sql.hive.Json2Parquet\
  --conf spark.app.name="$EXPR_NAME"\
  --conf spark.approx.inputFiles="$INPUT_DATA"\
  --conf spark.approx.outputFiles="$OUTPUT_DATA"\
  examples/target/scala-2.10/spark-examples-1.4.3-SNAPSHOT-hadoop2.2.0.jar > "$LOG_FILE_NAME" 2>&1

