
#!/usr/bin/env bash

set -euo pipefail

CONTAINER="voltstream-kafka"
TOPIC="ev.telemetry.raw"

echo "Creating Kafka topic: $TOPIC"

docker exec "$CONTAINER" \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic "$TOPIC" \
  --partitions 3 \
  --replication-factor 1

echo "Verifying Kafka topic..."

docker exec "$CONTAINER" \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic "$TOPIC"

echo "Kafka topic setup complete."
