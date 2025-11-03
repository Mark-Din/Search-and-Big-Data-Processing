#!/bin/bash
echo "⏳ Waiting for Kafka broker to start..."
sleep 10

echo "✅ Creating Kafka topics..."
kafka-topics --bootstrap-server kafka:9092 \
  --create --topic connect-offsets \
  --partitions 25 --replication-factor 1 \
  --config cleanup.policy=compact || true

kafka-topics --bootstrap-server kafka:9092 \
  --create --topic connect-status \
  --partitions 1 --replication-factor 1 \
  --config cleanup.policy=compact || true

kafka-topics --bootstrap-server kafka:9092 \
  --create --topic connect-configs \
  --partitions 1 --replication-factor 1 \
  --config cleanup.policy=compact || true

# echo "✅ Kafka topics created. Starting broker..."
# exec /etc/confluent/docker/run
