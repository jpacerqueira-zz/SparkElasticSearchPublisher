#!/bin/bash
MY_FOLDER="/opt/streamsets-datacollector/workflow-scripts/customerconsolidation"
##
#
DATE_V1=$1
if [ -z "$DATE_V1" ]
then
 DATE_V1=20190225
fi
DATE_V2=$2
if [ -z "$DATE_V2" ]
then
 DATE_V2=2019-02-24-*
fi
##
MASTER_URL=yarn
NUM_EXECUTORS=1
DRIVER_MEMORY=512m
EXECUTOR_MEMORY=256m
EXECUTOR_CORES=2
##
echo "DROP PARTITON" > $MY_FOLDER/execute-customerconsolidation-raw-toHiveTable-lastrun.log
hdfs dfs -rm -r -f -skipTrash /data/staged/customerconsolidation/customer/dt=${DATE_V1} >> $MY_FOLDER/execute-customerconsolidation-raw-toHiveTable-lastrun.log
hdfs dfs -mkdir -p /data/staged/customerconsolidation/customer/dt=${DATE_V1} >> $MY_FOLDER/execute-customerconsolidation-raw-toHiveTable-lastrun.log

spark-submit  --master ${MASTER_URL} --num-executors ${NUM_EXECUTORS} --driver-memory ${DRIVER_MEMORY} --executor-memory ${EXECUTOR_MEMORY} --executor-cores ${EXECUTOR_CORES}  $MY_FOLDER/import-customerconsolidation-raw-toHiveTable.py --datev1 ${DATE_V1} --datev2 ${DATE_V2} >> $MY_FOLDER/execute-customerconsolidation-raw-toHiveTable-lastrun.log
#
