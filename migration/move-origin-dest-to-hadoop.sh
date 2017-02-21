#!/bin/bash
DATA_FOLDER=$1
HDFS_TARGET_FOLDER=$2

if [ ! $HADOOP_HOME ]; then
	echo "HADOOP_HOME unset"
	exit -1
fi

if [ ! $HDFS_TARGET_FOLDER ]; then
	echo "HDFS_TARGET_FOLDER unset"
	exit -1
fi

if [ ! -d $DATA_FOLDER ]; then
	echo "${DATA_FOLDER} not found"
	exit -1
fi

for FILE in `ls $DATA_FOLDER/airline_origin_destination/*.zip`; do
	for CSV_NAME in `unzip -l $FILE  | grep csv | tr -s ' ' | cut -d ' ' -f4`; do
		unzip -p $FILE $CSV_NAME | $HADOOP_HOME/bin/hdfs dfs -put - $HDFS_TARGET_FOLDER/$CSV_NAME
		echo "$CSV_NAME from $FILE_NAME ready in $HDFS_TARGET_FOLDER"
	done 
done
