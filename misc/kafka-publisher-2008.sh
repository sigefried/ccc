#! /bin/bash

source ./export-env.sh

for f in $(hdfs dfs -find '/input/*2008*.bz2'); do
  echo "publish $f"
  hdfs dfs -cat $f | bzcat | \
    /home/ubuntu/kafka/bin/kafka-console-producer.sh \
    --broker-list ${brokerlists} --topic ${topic_2008}
#  sleep 0.5
done
