#!/bin/bash
set -e

CONTAINER_NAME="namenode"
INPUT_PATH="/data/imdb/title.principals.tsv"
RATINGS_FILE="datasets/title.ratings.tsv"
OUTPUT_PATH="/output"
CONTAINER_SCRIPT_PATH="/tmp/hadoop-scripts"

# Ensure the script directory exists in the container
docker exec "$CONTAINER_NAME" mkdir -p "$CONTAINER_SCRIPT_PATH" || { echo "Failed to create script directory"; exit 1; }

# Deleting existing output directory if it exists, otherwise the job will fail
docker exec "$CONTAINER_NAME" hadoop fs -rm -r "$OUTPUT_PATH" 2>/dev/null || true

echo "Running job..."

# Copy scripts and ratings dataset into the container
docker cp "average_rating_per_actor_mapreduce.py" "$CONTAINER_NAME:$CONTAINER_SCRIPT_PATH/average_rating_per_actor_mapreduce.py"
docker cp "$RATINGS_FILE" "$CONTAINER_NAME:$CONTAINER_SCRIPT_PATH/title.ratings.tsv"

# Execute the Hadoop streaming job
docker exec "$CONTAINER_NAME" python3 \
    "$CONTAINER_SCRIPT_PATH/average_rating_per_actor_mapreduce.py" \
    -r hadoop \
    --ratings-file "$CONTAINER_SCRIPT_PATH/title.ratings.tsv" \
    hdfs://$INPUT_PATH \
    --output-dir hdfs://$OUTPUT_PATH

echo "Process completed."

# Retrieve the output file from HDFS
docker exec "$CONTAINER_NAME" hadoop fs -get /output/part-00000 .

# Copy the output file from the container to local machine
docker cp "$CONTAINER_NAME":/part-00000 outputs/mapreduce-average_rating_per_actor.txt

echo "Resulting file available in the /outputs folder."
