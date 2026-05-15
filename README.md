# Kafka KRaft + Java Producer Consumer + Kafka UI + Schema Registry

This project demonstrates a complete Apache Kafka setup using:

- Apache Kafka in KRaft Mode (Without ZooKeeper)
- Confluent Kafka Docker Images
- Kafka UI
- Schema Registry
- Java Kafka Producer
- Java Kafka Consumer
- Topic-to-topic event publishing

---

# Tech Stack

- Java 17
- Apache Kafka
- Docker
- Docker Compose
- Confluent Platform
- Maven

---

# Project Architecture

OrderProducer
↓
orders-topic
↓
OrderConsumer
↓
Process Message
↓
processed-orders-topic

---

# Features

- Kafka KRaft setup
- No ZooKeeper
- Kafka UI integration
- Schema Registry integration
- Producer publishes messages to Kafka
- Consumer reads messages from Kafka
- Consumer republishes processed messages to another topic
- Dockerized local setup

---

# Kafka Topics Used

| Topic Name | Purpose |
|---|---|
| orders-topic | Receives original order events |
| processed-orders-topic | Stores processed order events |

---

# Consumer Group

```text
order-group