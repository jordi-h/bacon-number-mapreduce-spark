#!/bin/bash
set -e

CONTAINER_NAME="namenode"
INPUT_PATH="/data/imdb/title.principals.tsv"
OUTPUT_PATH="./output"
CONTAINER_SCRIPT_PATH="/tmp/hadoop-scripts"

# Ensure the script directory exists in the container
docker exec "$CONTAINER_NAME" mkdir -p "$CONTAINER_SCRIPT_PATH" || { echo "Failed to create script directory"; exit 1; }

# Deleting existing output directory if it exists
docker exec "$CONTAINER_NAME" hadoop fs -rm -r "$OUTPUT_PATH" 2>/dev/null || true

echo "Running job..."

# Copy mapper and reducer scripts into the container
docker cp "mapper.py" "$CONTAINER_NAME:$CONTAINER_SCRIPT_PATH/mapper.py"
docker cp "reducer.py" "$CONTAINER_NAME:$CONTAINER_SCRIPT_PATH/reducer.py"

# Execute the Hadoop streaming job
docker exec "$CONTAINER_NAME" hadoop jar /opt/hadoop-3.2.1/share/hadoop/tools/lib/hadoop-streaming-3.2.1.jar \
    -file "$CONTAINER_SCRIPT_PATH/mapper.py" \
    -mapper "python3 mapper.py" \
    -file "$CONTAINER_SCRIPT_PATH/reducer.py" \
    -reducer "python3 reducer.py" \
    -input "$INPUT_PATH" \
    -output "$OUTPUT_PATH"

echo "Process completed."


# hdfs dfs -get ./output/part-00000 tmp/
# /mnt/c/Users/hoore/Documents/ULiege/Q2/TDS/2.Assignments/A2/bacon-number-mapreduce-spark/workdir