#! /bin/bash

source ./export-env.sh

/home/ubuntu/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server ${brokerlists} --from-beginning \
  --topic ${topic}
