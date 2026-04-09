#!/bin/bash
echo "Waiting spark is ready..."
sleep 15
echo "Launching spark consumer..."
spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
  /opt/bitnami/spark/jobs/consumer.py