#! /bin/bash
set -x
hdfs dfs -rm -r /ccc-tmp/task1-3-q2-tmp
hadoop jar ../ccc-1.0-SNAPSHOT.jar task1.group3.q2.FindFlightsSchedule /input
