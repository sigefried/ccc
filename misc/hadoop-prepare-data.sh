#! /bin/bash

hdfs dfs -rm -r /import_output
hdfs dfs -mkdir /input

hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
-D mapred.reduce.tasks=0 \
-D mapred.map.tasks.speculative.execution=false \
-D mapred.task.timeout=1000000 \
-input /directories.txt \
-inputformat org.apache.hadoop.mapred.lib.NLineInputFormat \
-output /import_output \
-mapper /home/ubuntu/ccc/misc/prepare_input.sh
