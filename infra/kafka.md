# Kafka Network Configuration Guide

This infrastructure uses a dual-listener configuration to allow Kafka to communicate with both internal Docker containers and external applications (like a local Spark cluster) without routing conflicts.

## The 3 Listeners Explained

Kafka is configured with three distinct "doors" (Listeners), mapped to the `PLAINTEXT` protocol (unencrypted):

1. **`INTERNAL` (Port 29092)**
   * **For:** Docker containers on the same network (Node.js API).
   * **Broker Address:** `kafka:29092`
   * *Why?* Keeps traffic securely inside the Docker network.

2. **`EXTERNAL` (Port 9092)**
   * **For:** Applications running on the host machine outside Docker infra container (Spark Producers/Consumers).
   * **Broker Address:** `localhost:9092` (or whatever `KAFKA_PORT` is set to in `.env`).
   * *Why?* Allows external scripts to reach Kafka without getting lost in Docker's internal routing.

3. **`CONTROLLER` (Port 9093)**
   * **For:** Kafka's internal cluster management (KRaft mode).
   * *Note:* You will never connect your applications to this port.

## Rules for Developers

* If your code runs **INSIDE** the same Docker container as kafka connect to `kafka:29092`
* If your code runs **OUTSIDE** on your PC connect to `localhost:9092`