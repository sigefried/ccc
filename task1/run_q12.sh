#! /bin/bash
hdfs dfs -rm -r /q12
hadoop jar ../ccc-1.0-SNAPSHOT.jar task1.group1.AirLineDelay /input /q12

printf "\n\nget result..."
hdfs dfs -get /q12

printf "\n\ntop 10 on-time departure airline:\n"
cat ./q12/part-r-00000 | sort -k 2 -n | head -10

