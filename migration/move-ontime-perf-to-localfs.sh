#!/bin/bash
DATA_FOLDER=$1
TARGET_FOLDER=$2

if [ ! $TARGET_FOLDER ]; then
	echo "TARGET_FOLDER unset"
	exit -1
fi

if [ ! -d $DATA_FOLDER ]; then
	echo "${DATA_FOLDER} not found"
	exit -1
fi

for FILE in `ls $DATA_FOLDER/airline_ontime/*/*.zip`; do
	for CSV_NAME in `unzip -l $FILE  | grep csv | tr -s ' ' | cut -d ' ' -f4`; do
		unzip -p $FILE $CSV_NAME > $TARGET_FOLDER/$CSV_NAME
		echo "$CSV_NAME from $FILE_NAME ready in $TARGET_FOLDER"
	done 
done
