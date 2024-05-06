#!/bin/bash
set -e

CONTAINER_NAME="namenode"
INPUT_PATH="/data/imdb/title.principals.millions.tsv"
OUTPUT_PATH="/output"
CONTAINER_SCRIPT_PATH="/tmp/hadoop-scripts"

# Defaults
SOURCE_NODE="--source nm0000102"
DEPTH="--depth 6"

# Process command line options
while getopts s:d: option
do
    case "${option}" in
        s) SOURCE_NODE="--source ${OPTARG}";;
        d) DEPTH="--depth ${OPTARG}";;
    esac
done

# Ensure the script directory exists in the container
docker exec "$CONTAINER_NAME" mkdir -p "$CONTAINER_SCRIPT_PATH" || { echo "Failed to create script directory"; exit 1; }

# Deleting existing output directory if it exists, otherwise the job will fail
docker exec "$CONTAINER_NAME" hadoop fs -rm -r "$OUTPUT_PATH" 2>/dev/null || true

echo "Running job..."

# Copy scripts into the container
docker cp "parallel_bfs_mapreduce.py" "$CONTAINER_NAME:$CONTAINER_SCRIPT_PATH/parallel_bfs_mapreduce.py"

# Execute the Hadoop streaming job
docker exec "$CONTAINER_NAME" python3 \
    "$CONTAINER_SCRIPT_PATH/parallel_bfs_mapreduce.py" \
    -r hadoop \
    hdfs://$INPUT_PATH \
    --output-dir hdfs://$OUTPUT_PATH \
    $SOURCE_NODE \
    $DEPTH

echo "Process completed."

# Retrieve the output file from HDFS
docker exec "$CONTAINER_NAME" hadoop fs -get /output/part-00000 .

# Copy the output file from the container to local machine
docker cp "$CONTAINER_NAME":/part-00000 outputs/mapreduce-bacon_number.txt

echo "Resulting file available in the /outputs folder."

# head -n 2000000 datasets/title.principals.tsv > datasets/title.principals.millions.tsv