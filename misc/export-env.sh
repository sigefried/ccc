#! /bin/bash
set -x
export zookeepers="localhost:2181"
export brokerlists="localhost:9092"

# export zookeepers="ip-172-30-0-10.ec2.internal:2181,ip-172-30-0-59.ec2.internal:2181,ip-172-30-0-156.ec2.internal:2181"
# export brokerlists="localhost:9092"

export topic="performance"
export topic_2008="performance2008"
