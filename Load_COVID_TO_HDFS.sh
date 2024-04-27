#!/bin/bash

# Define the paths
LINUX_LANDING_AREA=/home/cloudera/covid_project/landing_zone/COVID_SRC_LZ
HDFS_LZ=/user/cloudera/ds/COVID_HDFS_LZ

# Display the paths
echo "GLOBAL Variables: Linux Landing Area - $LINUX_LANDING_AREA, HDFS Landing Zone - $HDFS_LZ"

# Create the directory in HDFS
hdfs dfs -mkdir -p $HDFS_LZ
echo "COVID_HDFS_LZ created successfully in HDFS"

# Load the file into HDFS
hdfs dfs -put $LINUX_LANDING_AREA/covid-19.csv $HDFS_LZ
echo "covid-19.csv dataset loaded successfully into HDFS"
