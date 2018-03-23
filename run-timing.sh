#!/bin/bash

bin/spark-submit\
  --master local[*]\
  --driver-memory 40g\
  --executor-memory 40g\
  --class org.apache.spark.examples.sql.hive.TestIolapPR\
  --conf spark.approx.logDir="/disk/local/disk2/andrew/logdir"\
  --conf spark.approx.inputFiles="/disk/local/disk2/andrew/data/students1g.json"\
  --conf spark.eventLog.enabled="true"\
  --conf spark.eventLog.dir="/disk/local/disk2/andrew/logdir"\
  examples/target/scala-2.10/spark-examples-1.4.3-SNAPSHOT-hadoop2.2.0.jar 2>&1 | tee timing.log

