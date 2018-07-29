#! /bin/bash
hdfs dfs -rm -r /ccc-tmp/task1-2-q2-tmp
hadoop jar ../ccc-1.0-SNAPSHOT.jar task1.group2.q2.TopRankDestWithOrigin /input

