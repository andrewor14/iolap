#!/bin/bash

LOG_DIR="${LOG_DIR:-/disk/local/disk2/stafman/logs}"
OUTPUT_DATA="${OUTPUT_DATA:-/home/stafman/generate_16k.parquet}"
EXPR_NAME="${EXPR_NAME:-generateparquet}"
RUNTIME_MEMORY="${RUNTIME_MEMORY:-45g}"

# Print all flags to log file
LOG_FILE_NAME="$LOG_DIR/$EXPR_NAME".log
# Run Spark
bin/spark-submit\
  --master local[*]\
  --driver-memory "$RUNTIME_MEMORY"\
  --executor-memory "$RUNTIME_MEMORY"\
  --class org.apache.spark.examples.sql.hive.GenerateParquet\
  --conf spark.app.name="$EXPR_NAME"\
  --conf spark.approx.outputFile="$OUTPUT_DATA"\
  examples/target/scala-2.10/spark-examples-1.4.3-SNAPSHOT-hadoop2.2.0.jar > "$LOG_FILE_NAME" 2>&1

