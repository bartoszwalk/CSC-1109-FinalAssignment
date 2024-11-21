#!/bin/bash

HDFS_DIR="/user/hive/warehouse/cleaned_accidents"
LOCAL_DIR="results/cleaned_accidents"

echo "Deleting existing files in HDFS directory: $HDFS_DIR"
hdfs dfs -rm -r -skipTrash $HDFS_DIR/*

echo "Creating HDFS directory: $HDFS_DIR"
hdfs dfs -mkdir -p $HDFS_DIR

echo "Copying data from $LOCAL_DIR to $HDFS_DIR"
hdfs dfs -put $LOCAL_DIR/* $HDFS_DIR

echo "Listing files in HDFS directory $HDFS_DIR"
hdfs dfs -ls $HDFS_DIR

echo "Data loading completed."
