#! /bin/bash
set -x

script=$1

spark-submit \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.2 \
--master local[2] \
--deploy-mode client \
$script
