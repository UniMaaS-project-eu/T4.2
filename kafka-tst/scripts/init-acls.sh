#!/bin/bash
set -e

echo "Waiting for Kafka..."
sleep 30

cat >/tmp/admin.properties <<EOF
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="${KAFKA_ADMIN_PASSWORD}";
EOF

cd /opt/kafka

echo "Creating topic..."
bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --command-config /tmp/admin.properties \
  --create \
  --if-not-exists \
  --topic sensor-data-1 \
  --partitions 1 \
  --replication-factor 1

echo "Creating ACLs..."

bin/kafka-acls.sh \
  --bootstrap-server kafka:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal User:consumer \
  --operation READ \
  --operation DESCRIBE \
  --topic sensor-data-1

bin/kafka-acls.sh \
  --bootstrap-server kafka:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal User:publisher \
  --operation WRITE \
  --operation DESCRIBE \
  --topic sensor-data-1

bin/kafka-acls.sh \
  --bootstrap-server kafka:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal User:consumer \
  --operation READ \
  --group '*'

bin/kafka-acls.sh \
  --bootstrap-server kafka:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal User:publisher \
  --operation DESCRIBE \
  --cluster

bin/kafka-acls.sh \
  --bootstrap-server kafka:9092 \
  --command-config /tmp/admin.properties \
  --add \
  --allow-principal User:consumer \
  --operation DESCRIBE \
  --cluster

echo "ACLs created"