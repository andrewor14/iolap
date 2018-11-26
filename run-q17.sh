#!/bin/bash

LOG_DIR="${LOG_DIR:-/disk/local/disk2/stafman/logs}"
EXPR_NAME="${EXPR_NAME:-tpchq17}"
RUNTIME_MEMORY="${RUNTIME_MEMORY:-45g}"

# Print all flags to log file
LOG_FILE_NAME="$LOG_DIR/$EXPR_NAME".log
# Run Spark
bin/spark-submit\
  --packages com.databricks:spark-csv_2.10:1.3.0\
  --master local[*]\
  --driver-memory "$RUNTIME_MEMORY"\
  --executor-memory "$RUNTIME_MEMORY"\
  --class org.apache.spark.examples.sql.hive.TPCHQ17\
  --conf spark.app.name="$EXPR_NAME"\
  --conf spark.sql.autoBroadcastJoinThreshold=-1\
  examples/target/scala-2.10/spark-examples-1.4.3-SNAPSHOT-hadoop2.2.0.jar > "$LOG_FILE_NAME" 2>&1

