#!/bin/bash

CONTAINER_NAME="namenode"

# Copy datasets to container
docker cp datasets/title.principals.tsv $CONTAINER_NAME:title.principals.tsv
docker cp datasets/title.ratings.tsv $CONTAINER_NAME:title.ratings.tsv

# Create a directory in HDFS for storing the data
docker exec namenode hdfs dfs -mkdir -p /data/imdb

# Upload the file from the container to the HDFS directory
docker exec namenode hdfs dfs -put title.principals.tsv /data/imdb/title.principals.tsv
docker exec namenode hdfs dfs -put title.ratings.tsv /data/imdb/title.ratings.tsv
