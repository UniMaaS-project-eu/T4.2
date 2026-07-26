#!/bin/bash
set -e

cat >/tmp/server.properties <<EOF
process.roles=broker,controller
node.id=1

controller.listener.names=CONTROLLER
controller.quorum.voters=1@kafka:9094

listeners=INTERNAL://0.0.0.0:9092,EXTERNAL://0.0.0.0:9093,CONTROLLER://0.0.0.0:9094
advertised.listeners=INTERNAL://kafka:9092,EXTERNAL://kafka:9093
listener.security.protocol.map=INTERNAL:SASL_PLAINTEXT,EXTERNAL:SASL_PLAINTEXT,CONTROLLER:PLAINTEXT
inter.broker.listener.name=INTERNAL

sasl.enabled.mechanisms=SCRAM-SHA-512
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512
listener.name.internal.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="${KAFKA_ADMIN_PASSWORD}";
listener.name.external.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="${KAFKA_ADMIN_PASSWORD}";

authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
super.users=User:admin;User:ANONYMOUS
allow.everyone.if.no.acl.found=false

offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
auto.create.topics.enable=true
EOF

if [ ! -f /tmp/kafka-logs/meta.properties ]; then
  /opt/kafka/bin/kafka-storage.sh format \
    --ignore-formatted \
    --cluster-id "${KAFKA_CLUSTER_ID}" \
    --config /tmp/server.properties \
    --add-scram "SCRAM-SHA-512=[name=admin,password=${KAFKA_ADMIN_PASSWORD}]" \
    --add-scram "SCRAM-SHA-512=[name=publisher,password=${KAFKA_PUBLISHER_PASSWORD}]" \
    --add-scram "SCRAM-SHA-512=[name=consumer,password=${KAFKA_CONSUMER_PASSWORD}]"
fi

exec /opt/kafka/bin/kafka-server-start.sh /tmp/server.properties