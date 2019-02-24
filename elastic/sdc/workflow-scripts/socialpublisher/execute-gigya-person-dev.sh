#!/bin/bash

MAIN_CLASS1=com.jpac.scv.publishing.elastic.ElasticAggregatorSpark
MAIN_CLASS2=com.jpac.scv.publishing.elastic.ElasticSparkPublisher
APP_JAR=elastic-1.0-SNAPSHOT-jar-with-dependencies.jar
MASTER_URL=yarn
NUM_EXECUTORS=1
DRIVER_MEMORY=512m
EXECUTOR_MEMORY=256m
EXECUTOR_CORES=2
APP_CONF_FILE=later-todo.conf

if [ $# -lt 1 ]
then
  DATE_STRING=`date -d "1 day ago" '+%Y%m%d'`
  DATE_HDFS=`date -d "1 day ago" '+%Y-%m-%d'`
else
  DATE_STRING=$1
fi

# Clean hdfs results to avoid issues with mappings while job computes
hdfs dfs -mkdir -p /data/raw/gfans/gfans/person/dt=0
hdfs dfs -mkdir -p /data/staged/gfans/person/dt=0
hdfs dfs -mkdir -p /data/published/gfans/person/dt=0

# Copy local empty test File
hdfs dfs -mkdir -p /data/raw/gfans/person/dt=${DATE_HDFS}
hdfs dfs -copyFromLocal person_gfans_clean.json /data/raw/gfans/person/dt=${DATE_HDFS}

# Clean hdfs results to avoid issues with mappings while job computes
hdfs dfs -rm -skipTrash -f /data/staged/gfans/person/dt=${DATE_HDFS}/*
hdfs dfs -rm -skipTrash -f /data/published/gfans/person/dt=${DATE_HDFS}/*
# 
# submit from client to produce staged and publishing data
spark-submit --class ${MAIN_CLASS1} --master ${MASTER_URL} --num-executors ${NUM_EXECUTORS} --driver-memory ${DRIVER_MEMORY} --executor-memory ${EXECUTOR_MEMORY} --executor-cores ${EXECUTOR_CORES} ${APP_JAR} --dthr ${DATE_STRING}

# load ES mapping
sudo bash -x dev-scv-person-mapping.sh ${DATE_STRING}

# submit from client to build the daily ES index person 
spark-submit --class ${MAIN_CLASS2} --master ${MASTER_URL} --num-executors ${NUM_EXECUTORS} --driver-memory ${DRIVER_MEMORY} --executor-memory ${EXECUTOR_MEMORY} --executor-cores ${EXECUTOR_CORES} ${APP_JAR} --dthr ${DATE_STRING}
