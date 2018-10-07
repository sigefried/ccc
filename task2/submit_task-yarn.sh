#! /bin/bash
set -x

script=$1

spark-submit \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.2 \
--master yarn \
--deploy-mode client \
--driver-memory 4g \
--driver-cores 4 \
--executor-memory 3g \
--num-executors 10 \
--executor-cores 2 \
--conf spark.task.cpus=1 \
--conf spark.yarn.executor.memoryOverhead=2000 \
--conf spark.default.parallelism=21 \
$script
