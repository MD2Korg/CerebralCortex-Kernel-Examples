#!/usr/bin/env bash

# Python3 path
export PYSPARK_PYTHON=/usr/bin/python3

#Spark path
export SPARK_HOME=/MAIN-DIR-PATH/spark-2.4.0-bin-hadoop2.7/

#PySpark args (do not change unless you know what you are doing)
export PYSPARK_SUBMIT_ARGS="--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 pyspark-shell"

#set spark home
export PATH=$SPARK_HOME/bin:$PATH



# spark master
SPARK_MASTER="local[*]"

spark-submit --conf spark.streaming.kafka.maxRatePerPartition=10 --driver-memory 1g --executor-memory 1g --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 main.py