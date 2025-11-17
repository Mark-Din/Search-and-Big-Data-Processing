#!/bin/bash

echo "⏳ Waiting for Kafka broker to become ready..."

# Wait until kafka responds to metadata request
while ! kafka-topics --bootstrap-server kafka_qidu:9092 --list >/dev/null 2>&1; do
    echo "   Kafka is not ready yet... retrying in 3 seconds."
    sleep 3
done

echo "✅ Kafka is ready! Creating topics..."

# Create topics with retries (safe even if they already exist)
# --bootstrap-server xxxx must match KAFKA_ADVERTISED_LISTENERS set in docker-compose
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

echo "🎉 Kafka topics created. Starting broker..."

exec /etc/confluent/docker/run
