#!/bin/bash
echo "Waiting spark is ready..."
sleep 15
echo "Launching spark consumer..."
spark-submit \
          --master spark://spark-master:7077 \
          --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,mysql:mysql-connector-java:8.0.33 \
          /opt/bitnami/spark/jobs/consumer.py