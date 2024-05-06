#!/bin/bash

CONTAINER_NAME="namenode"

# Copy datasets to container
docker cp datasets/title.principals.tsv $CONTAINER_NAME:title.principals.tsv
docker cp datasets/title.principals.millions.tsv $CONTAINER_NAME:title.principals.millions.tsv
docker cp datasets/title.ratings.tsv $CONTAINER_NAME:title.ratings.tsv
#docker cp datasets/smallest.tsv $CONTAINER_NAME:smallest.tsv

# Create a directory in HDFS for storing the data
docker exec namenode hdfs dfs -mkdir -p /data/imdb

# Upload the file from the container to the HDFS directory
docker exec namenode hdfs dfs -put title.principals.tsv /data/imdb/title.principals.tsv
docker exec namenode hdfs dfs -put title.principals.millions.tsv /data/imdb/title.principals.millions.tsv
docker exec namenode hdfs dfs -put title.ratings.tsv /data/imdb/title.ratings.tsv
#docker exec namenode hdfs dfs -put smallest.tsv /data/imdb/smallest.tsv
