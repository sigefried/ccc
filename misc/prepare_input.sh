#! /bin/bash

read id dir_name
local_dir=/home/ubuntu/tmp/${id}
hadoop_input=/input

echo "local_dir: $local_dir}" 1>&2
echo "id: ${local_dir}"


echo "create tmp dir $local_dir..." 1>&2
mkdir -p ${local_dir}

echo "copy the data from master to tmp..." 1>&2
scp -r master:${dir_name}/* ${local_dir}


for f in ${local_dir}/*.zip; do
	echo "process file $f..." 1>&2
	/home/ubuntu/ccc/misc/prepare_input.py $f
	echo "dump file to hdfs ... " 1>&2
	$HADOOP_HOME/bin/hadoop fs -put ${f%.zip}.bz2 $hadoop_input/
done

echo "DONE" 1>&2


