#!/bin/bash


#LOG_DIR="${LOG_DIR:-/disk/local/disk2/andrew/logs}"
#INPUT_DATA="${INPUT_DATA:-/disk/local/disk2/andrew/data/students1g.json}"

LOG_DIR="${LOG_DIR:-/disk/local/disk2/stafman/logs}"
INPUT_DATA="${INPUT_DATA:-data/students1g.json}"
SHOULD_CACHE_TABLES="${SHOULD_CACHE_TABLES:-true}"
EXPR_NAME="${EXPR_NAME:-timing}"

bin/spark-submit\
  --master local[*]\
  --driver-memory 45g\
  --executor-memory 45g\
  --class org.apache.spark.examples.sql.hive.TestIolapPR\
  --conf spark.app.name="$EXPR_NAME"\
  --conf spark.approx.logDir="$LOG_DIR"\
  --conf spark.approx.inputFiles="$INPUT_DATA"\
  --conf spark.approx.shouldCacheTables="$SHOULD_CACHE_TABLES"\
  --conf spark.eventLog.enabled="true"\
  --conf spark.eventLog.dir="$LOG_DIR"\
  examples/target/scala-2.10/spark-examples-1.4.3-SNAPSHOT-hadoop2.2.0.jar > "$LOG_DIR/$EXPR_NAME".log 2>&1

