#!/bin/bash

LOG_DIR="${LOG_DIR:-/disk/local/disk2/andrew/logs}"
INPUT_DATA="${INPUT_DATA:-/disk/local/disk2/andrew/data/students.json}"
#LOG_DIR="${LOG_DIR:-/disk/local/disk2/stafman/logs}"
#INPUT_DATA="${INPUT_DATA:-data/students1g.json}"
NUM_BATCHES="${NUM_BATCHES:-40}"
NUM_PARTITIONS="${NUM_PARTITIONS:-16000}"
NUM_BOOTSTRAP_TRIALS="${NUM_BOOTSTRAP_TRIALS:-100}"
SHOULD_CACHE_TABLES="${SHOULD_CACHE_TABLES:-true}"
IOLAP_CACHE_ENABLED="${IOLAP_CACHE_ENABLED:-true}"
EXPR_NAME="${EXPR_NAME:-timing}"
RUNTIME_MEMORY="${RUNTIME_MEMORY:-45g}"

# Print all flags to log file
LOG_FILE_NAME="$LOG_DIR/$EXPR_NAME".log
echo "===========================================================================
  LOG_DIR=$LOG_DIR
  INPUT_DATA=$INPUT_DATA
  NUM_BATCHES=$NUM_BATCHES
  NUM_PARTITIONS=$NUM_PARTITIONS
  NUM_BOOTSTRAP_TRIALS=$NUM_BOOTSTRAP_TRIALS
  SHOULD_CACHE_TABLES=$SHOULD_CACHE_TABLES
  IOLAP_CACHE_ENABLED=$IOLAP_CACHE_ENABLED
  EXPR_NAME=$EXPR_NAME
  RUNTIME_MEMORY=$RUNTIME_MEMORY
===========================================================================
" > "$LOG_FILE_NAME"

# Run Spark
bin/spark-submit\
  --master local[*]\
  --driver-memory "$RUNTIME_MEMORY"\
  --executor-memory "$RUNTIME_MEMORY"\
  --class org.apache.spark.examples.sql.hive.TestIolapPR\
  --conf spark.app.name="$EXPR_NAME"\
  --conf spark.approx.logDir="$LOG_DIR"\
  --conf spark.approx.inputFiles="$INPUT_DATA"\
  --conf spark.approx.numBatches="$NUM_BATCHES"\
  --conf spark.approx.numPartitions="$NUM_PARTITIONS"\
  --conf spark.approx.numBootstrapTrials="$NUM_BOOTSTRAP_TRIALS"\
  --conf spark.approx.shouldCacheTables="$SHOULD_CACHE_TABLES"\
  --conf spark.approx.iolapCacheEnabled="$IOLAP_CACHE_ENABLED"\
  --conf spark.eventLog.enabled="true"\
  --conf spark.eventLog.dir="$LOG_DIR"\
  examples/target/scala-2.10/spark-examples-1.4.3-SNAPSHOT-hadoop2.2.0.jar >> "$LOG_FILE_NAME" 2>&1

