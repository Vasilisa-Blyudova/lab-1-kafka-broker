#!/usr/bin/env bash

set -e

BOOTSTRAP_SERVERS="broker-1:9092,broker-2:9092,broker-3:9092"

echo "Waiting for Kafka cluster to be fully ready..."
until kafka-broker-api-versions.sh --bootstrap-server broker-1:9092 > /dev/null 2>&1; do
  sleep 2
done

echo "Kafka cluster is ready. Creating topics..."

kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVERS}" \
  --create --if-not-exists \
  --topic raw-data \
  --partitions 3 \
  --replication-factor 3

kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVERS}" \
  --create --if-not-exists \
  --topic processed-data \
  --partitions 3 \
  --replication-factor 3

kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVERS}" \
  --create --if-not-exists \
  --topic results \
  --partitions 3 \
  --replication-factor 3

echo "Topics created:"
kafka-topics.sh --bootstrap-server "${BOOTSTRAP_SERVERS}" --list
