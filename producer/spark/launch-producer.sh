#!/bin/bash

echo "Waiting for Spark to initialize..."
sleep 10

echo "Launching data generation..."
spark-submit \
          --master spark://spark-master-producer:7077 \
          /opt/bitnami/spark/jobs/producer.py
