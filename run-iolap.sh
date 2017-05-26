#!/bin/bash

if [[ "$#" != "3" ]]; then
  echo "Usage: ./run-iolap.sh <use slaq> <num pools> <num bootstrap trials>"
  exit 1
fi

USE_SLAQ="$1"
NUM_POOLS="$2"
BOOTSTRAP_TRIALS="$3"

# Set scheduler name
SCHEDULER_NAME=""
if [[ "$USE_SLAQ" == "true" ]]; then
  SCHEDULER_NAME="slaq"
elif [[ "$USE_SLAQ" == "false" ]]; then
  SCHEDULER_NAME="fair"
else
  echo "Invalid argument <use slaq>: '$USE_SLAQ', expected 'true' or 'false'"
  exit 1
fi

# Construct comma delimited streamed relations, one for each pool
STREAMED_RELATIONS="pool1"
for i in $(seq 2 "$NUM_POOLS"); do
  STREAMED_RELATIONS="$STREAMED_RELATIONS,pool$i"
done

# Set output path
OUTPUT_DIR="data/$SCHEDULER_NAME"_"$NUM_POOLS"pools_"$BOOTSTRAP_TRIALS"bootstrap
LOG_FILE="$OUTPUT_DIR/output.log"
mkdir -p "$OUTPUT_DIR"
rm -rf "$OUTPUT_DIR/*"
echo "Logging to $LOG_FILE..."

# Run IOLAP
set -x
bin/spark-submit\
  --master local[*]\
  --driver-memory 40g\
  --executor-memory 40g\
  --conf spark.slaq.enabled="$USE_SLAQ"\
  --conf spark.slaq.intervalMs=5000\
  --conf spark.sql.online.number.bootstrap.trials="$BOOTSTRAP_TRIALS"\
  --conf spark.sql.online.streamed.relations="$STREAMED_RELATIONS"\
  --conf spark.naga.outputDir="$OUTPUT_DIR"\
  --conf spark.naga.intervalMs=30000\
  --class org.apache.spark.examples.sql.hive.RobertInTheFile\
  examples/target/scala-2.10/spark-examples-1.4.3-SNAPSHOT-hadoop2.2.0.jar 2>&1 | tee "$LOG_FILE"
set +x

