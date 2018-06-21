#! /bin/bash

dir_name=$2
local_dir=${HOME}/tmp/${1}
hadoop_input=/home/m/IdeaProjects/hadoop/input

echo "create tmp dir $local_dir..."
mkdir -p ${local_dir}

echo "copy the data from master to tmp..."
scp -r localhost:${dir_name}/* ${local_dir}

for f in ${local_dir}/*.zip; do
	echo "process file $f..."
	./prepare_input.py $f
	echo "dump file to hdfs ... "
	cp ${f%.zip}.bz2 $hadoop_input/
done

echo "DONE"


