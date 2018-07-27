#! /bin/bash
hdfs dfs -rm -r /q11
hadoop jar ../ccc-1.0-SNAPSHOT.jar task1.group1.TopAirport /input /q11

printf "\n\nget result..."
hdfs dfs -get /q11

printf "\n\ntop 10 most popular airports:"
cat ./q11/part-r-00000 | sort -k 2 -n -r | head -10

