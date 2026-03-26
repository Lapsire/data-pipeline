#!/bin/bash

echo "Waiting kafka starting..."
sleep 10

## DATA TOPIC
echo "Creating topics..."
echo "Data topic :${KAFKA_TOPIC_DATA}"
/opt/bitnami/kafka/bin/kafka-topics.sh --create --if-not-exists --topic ${KAFKA_TOPIC_DATA} --bootstrap-server kafka:29092 --partitions ${DATA_PARTITIONS} --replication-factor 1

## METRICS TOPIC
echo "Metrics topic :${KAFKA_TOPIC_METRICS}"
/opt/bitnami/kafka/bin/kafka-topics.sh --create --if-not-exists --topic ${KAFKA_TOPIC_METRICS} --bootstrap-server kafka:29092 --partitions ${METRICS_PARTITIONS} --replication-factor 1