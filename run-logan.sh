#!/bin/bash

bin/spark-submit --master local[*] --conf spark.scheduler.mode=FAIR --conf spark.executor.memory=50G --conf spark.driver.memory=50G --conf spark.memory.fraction=0.9 --conf spark.slaq.numPools=1 --conf spark.slaq.isFair=true --conf spark.sql.planner.externalSort=true --conf spark.slaq.logDir=/disk/local/disk1/andrew/iolap --class=org.apache.spark.examples.sql.hive.TestIolapPR examples/target/scala-2.10/spark-examples-1.4.3-SNAPSHOT-hadoop2.2.0.jar
