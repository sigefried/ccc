#! /bin/bash
hdfs dfs -rm -r /ccc-tmp/task1-2-q1-tmp
hadoop jar ../ccc-1.0-SNAPSHOT.jar task1.group2.q1.TopRankCarrierWithAirport /input

