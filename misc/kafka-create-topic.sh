#! /bin/bash
set -x

source ./export-env.sh

/home/ubuntu/kafka/bin/kafka-topics.sh --create --zookeeper ${zookeepers} \
--replication-factor 1 --partitions 21 --topic ${topic}
