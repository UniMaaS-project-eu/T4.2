# Kafka Docker Compose Setup

This project provides a local Apache Kafka setup with:

* **Kafka 4.2.0** running in **KRaft mode** (broker + controller in one node)
* **SASL/PLAIN authentication** for internal and external client connections
* **Kafka UI** for browsing topics, messages, and cluster metadata
* **ACL bootstrap container** to create topic, group, and cluster permissions automatically

## Services

### `kafka`

The main Kafka broker/controller node.

It exposes:

* `9092` for internal Docker network access
* `9093` for external access from the host machine
* `9094` for the KRaft controller listener (internal only)

Authentication is enabled with **SASL_PLAINTEXT** on the client-facing listeners.

### `kafka-ui`

A web UI for inspecting the Kafka cluster.

It connects to Kafka using the `consumer` user.

Accessible at:

```bash
http://localhost:8089
```

### `kafka-acl-init`

A one-time init container that waits for Kafka to start and then creates ACLs using the `admin` user.

It grants:

* `consumer`:

  * `READ`, `DESCRIBE` on topic `sensor-data-1`
  * `READ` on all consumer groups
  * `DESCRIBE` on the cluster
* `publisher`:

  * `WRITE`, `DESCRIBE` on topic `sensor-data-1`
  * `DESCRIBE` on the cluster

## Default users

The compose file defines these SASL/PLAIN users:

| Username    | Password        | Purpose                           |
| ----------- | --------------- | --------------------------------- |
| `admin`     | `admin-secret`  | Broker admin and ACL creation     |
| `publisher` | `client-secret` | Producer/write client             |
| `consumer`  | `client-secret` | Consumer/read client and Kafka UI |

## Topic and ACL behavior

* Topic auto-creation is enabled:

```yaml
KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
```

* ACL enforcement is enabled:

```yaml
KAFKA_ALLOW_EVERYONE_IF_NO_ACL_FOUND: "false"
```

This means users must have explicit permissions to access resources.

## Ports

| Port   | Service  | Description                                     |
| ------ | -------- | ----------------------------------------------- |
| `9092` | Kafka    | Internal listener for containers on `kafka-net` |
| `9093` | Kafka    | External listener for host clients              |
| `8089` | Kafka UI | Web interface                                   |

## How to start

Run:

```bash
docker compose up -d
```

To watch logs:

```bash
docker compose logs -f
```

To stop everything:

```bash
docker compose down
```

## Connection details

### From another container on the same Docker network

Use:

* **Bootstrap server:** `kafka:9092`
* **Security protocol:** `SASL_PLAINTEXT`
* **SASL mechanism:** `PLAIN`

### From the host machine

Use:

* **Bootstrap server:** `localhost:9093`
* **Security protocol:** `SASL_PLAINTEXT`
* **SASL mechanism:** `PLAIN`

## Example client configuration

### Producer (`publisher`)

```properties
bootstrap.servers=localhost:9093
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="publisher" password="client-secret";
```

### Consumer (`consumer`)

```properties
bootstrap.servers=localhost:9093
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="consumer" password="client-secret";
group.id=my-group
auto.offset.reset=earliest
```

## Kafka UI configuration

Kafka UI is configured to connect with:

* username: `consumer`
* password: `client-secret`
* bootstrap server: `kafka:9092`

Because it uses the `consumer` identity, UI actions are limited by that user's ACLs.

## Notes

* This setup uses **SASL_PLAINTEXT**, so credentials are not encrypted in transit.
* It is suitable for **local development and testing**, but not for production as-is.
* `KAFKA_SUPER_USERS` includes `User:admin` and `User:ANONYMOUS`.
* The `kafka-acl-init` container uses a fixed `sleep 30`, so on slower machines you may need to increase the wait time.
* This is a **single-node** Kafka cluster with replication factors set to `1`.

## Python client examples

Below are example Python clients for the `consumer` and `publisher` roles used in this compose setup.

### Install dependencies

```bash
pip install kafka-python paho-mqtt requests
```

## Consumer example

Below is the **Kafka-related configuration** from a Python consumer (using `kafka-python`).

```python
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'sensor-data-1',
    bootstrap_servers=['localhost:9093'],
    group_id='my-group',
    client_id='consumer-1',
    auto_offset_reset='latest',
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanism='PLAIN',
    sasl_plain_username='consumer',
    sasl_plain_password='client-secret',
)
```

### What this consumer needs

* Kafka bootstrap server: `localhost:9093`
* Username: `consumer`
* Password: `client-secret`
* Topic permission: `READ` and `DESCRIBE` on `sensor-data-1`
* Group permission: `READ` on the consumer group

## Publisher example

Below is the **Kafka-related configuration** from a Python producer (using `kafka-python`).

```python
from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers='localhost:9093',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanism='PLAIN',
    sasl_plain_username='publisher',
    sasl_plain_password='client-secret',
)

# Example send
producer.send('sensor-data-1', {'key': 'value'})
producer.flush()
```

### What this publisher needs

* Kafka bootstrap server: `localhost:9093`
* Username: `publisher`
* Password: `client-secret`
* Topic permission: `WRITE` and `DESCRIBE` on `sensor-data-1`

## Minimal Kafka-only examples

### Produce messages to `sensor-data-1`

```bash
docker exec -it kafka bash
```

Then run:

```bash
cat <<EOF >/tmp/publisher.properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="publisher" password="client-secret";
EOF

/opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka:9092 \
  --producer.config /tmp/publisher.properties \
  --topic sensor-data-1
```

### Consume messages from `sensor-data-1`

```bash
docker exec -it kafka bash
```

Then run:

```bash
cat <<EOF >/tmp/consumer.properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="consumer" password="client-secret";
EOF

/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --consumer.config /tmp/consumer.properties \
  --topic sensor-data-1 \
  --from-beginning
```

## Troubleshooting

### Kafka UI cannot connect

Check:

* Kafka is healthy and running
* `kafka-ui` is on the same Docker network
* the `consumer` credentials match the broker configuration
* ACLs allow the actions you expect

### ACLs were not created

Inspect the init container logs:

```bash
docker compose logs kafka-acl-init
```

If Kafka was not ready in time, restart the init container or increase the sleep delay.

### Host client cannot connect

Make sure your host client uses:

* `localhost:9093`
* `SASL_PLAINTEXT`
* `PLAIN`
* a valid username/password

### Python client issues

Common checks:

* `kafka-python` is installed
* your app is connecting to `localhost:9093` from the host, not `kafka:9092`
* the username matches the role you want to use
* the topic `sensor-data-1` exists or auto-create is enabled
* ACLs have already been applied by `kafka-acl-init`
* any dependent service like MQTT broker or AAS is reachable on the configured URL

## Security warning

This compose file contains plaintext credentials directly in environment variables. For real deployments, move secrets to a safer mechanism and use encrypted transport such as TLS.
