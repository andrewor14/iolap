#!/bin/bash

LOG_DIR="/disk/local/disk2/andrew/logs"
INPUT_DATA="/disk/local/disk2/andrew/data/students1g.json"

bin/spark-submit\
  --master local[*]\
  --driver-memory 40g\
  --executor-memory 40g\
  --class org.apache.spark.examples.sql.hive.TestIolapPR\
  --conf spark.approx.logDir="$LOG_DIR"\
  --conf spark.approx.inputFiles="$INPUT_DATA"\
  --conf spark.eventLog.enabled="true"\
  --conf spark.eventLog.dir="$LOG_DIR"\
  examples/target/scala-2.10/spark-examples-1.4.3-SNAPSHOT-hadoop2.2.0.jar 2>&1 | tee "$LOG_DIR/"timing.log

