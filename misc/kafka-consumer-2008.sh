#! /bin/bash
set -x

source ./export-env.sh

/home/ubuntu/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server ${brokerlists} --from-beginning \
  --topic ${topic_2008}
