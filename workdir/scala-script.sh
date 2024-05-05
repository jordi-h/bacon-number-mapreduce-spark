#!/bin/bash

if [ "$#" -ne 1 ]; then
  echo "Usage: $0 <ScalaScriptName>"
  echo "Do not specify the '.scala' extension, just the name (e.g. BaconNumber) !"
  exit 1
fi

CONTAINER_NAME="spark-master"
SCALA_SCRIPT_NAME=$1

DOCKER_CMD="docker exec -it $CONTAINER_NAME"

SPARK_CMD="cd /opt/info8002/ && /spark/bin/spark-shell \
  --master spark://spark-master:7077 \
  --conf spark.executor.memory=7g \
  --conf spark.driver.memory=4g \
  --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
  --conf spark.executor.memoryOverhead=1g"

SCALA_CMD=":load $SCALA_SCRIPT_NAME.scala
$SCALA_SCRIPT_NAME.main(Array.empty[String])"

$DOCKER_CMD bash -c "$SPARK_CMD <<EOF
$SCALA_CMD
EOF"
