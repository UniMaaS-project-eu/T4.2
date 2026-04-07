# Kafka Docker Compose Setup

This project provides a local Apache Kafka setup with:

* **Kafka 4.2.0** running in **KRaft mode** (broker + controller in one node)
* **SASL/SCRAM-SHA-512 authentication** for internal and external client connections
* **Kafka UI** for browsing topics, messages, and cluster metadata
* **ACL bootstrap container** to create topic, group, and cluster permissions automatically
* **Environment-based secrets** loaded from `.env`
* **Startup/init shell scripts** mounted from `./scripts`

## Services

### `kafka`

The main Kafka broker/controller node.

It uses:

* `.env` for passwords and cluster ID
* `/scripts/start-kafka.sh` to generate the broker config and start Kafka
* SCRAM credentials created at storage format time via `kafka-storage.sh --add-scram`

It exposes:

* `9092` for internal Docker network access
* `9093` for external access from the host machine
* `9094` for the KRaft controller listener (internal only)

Authentication is enabled with **SASL_PLAINTEXT** transport and SCRAM-SHA-512 as the SASL mechanism.

### `kafka-ui`

A web UI for inspecting the Kafka cluster.

It connects to Kafka using the `consumer` user SCRAM-SHA-512.

Accessible at:

```bash
http://localhost:8089
```

### `kafka-acl-init`

A one-time init container that waits for Kafka to start and then:

* authenticates as admin
* creates topic `sensor-data-1` if it does not exist
* applies ACLs for `consumer` and `publisher`

It uses `/scripts/init-acls.sh` and an admin client config built from `.env` values.

It grants:

* `consumer`:

  * `READ`, `DESCRIBE` on topic `sensor-data-1`
  * `READ` on all consumer groups
  * `DESCRIBE` on the cluster
* `publisher`:

  * `WRITE`, `DESCRIBE` on topic `sensor-data-1`
  * `DESCRIBE` on the cluster

## Default users

The setup defines these SCRAM users:

| Username    | Password        | Purpose                           |
| ----------- | --------------- | --------------------------------- |
| `admin`     | `admin-secret`  | Broker admin and ACL creation     |
| `publisher` | `client-secret` | Producer/write client             |
| `consumer`  | `client-secret` | Consumer/read client and Kafka UI |

## Authentication and ACL behavior

- Transport protocol:

```yaml
SASL_PLAINTEXT
```

- SASL mechanism:

```yaml
SCRAM-SHA-512
```

- Topic auto-creation is enabled on the broker:

```yaml
auto.create.topics.enable=true
```

- ACL enforcement is enabled:

```yaml
allow.everyone.if.no.acl.found=false
```

- SCRAM credentials are created during storage formatting in `start-kafka.sh`.

This means users must authenticate successfully and also have explicit permissions to access resources.

## Environment variables

Create a `.env` file with values like:

```env
KAFKA_CLUSTER_ID=5L6g3nShT-eMCtK--X86sw
KAFKA_ADMIN_PASSWORD=admin-secret
KAFKA_PUBLISHER_PASSWORD=publisher-secret
KAFKA_CONSUMER_PASSWORD=consumer-secret
```

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
* **SASL mechanism:** `SCRAM-SHA-512`

### From the host machine

Use:

* **Bootstrap server:** `localhost:9093`
* **Security protocol:** `SASL_PLAINTEXT`
* **SASL mechanism:** `SCRAM-SHA-512`

## Example client configuration

### Producer (`publisher`)

```properties
bootstrap.servers=localhost:9093
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="publisher" password="publisher-secret";
```

### Consumer (`consumer`)

```properties
bootstrap.servers=localhost:9093
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="consumer" password="consumer-secret";
group.id=my-group
auto.offset.reset=earliest
```

## Kafka UI configuration

Kafka UI is configured to connect with:

- username: `consumer`
- password: `${KAFKA_CONSUMER_PASSWORD}`
- bootstrap server: `kafka:9092`
- security protocol: `SASL_PLAINTEXT`
- sasl mechanism: `SCRAM-SHA-512`

Because it uses the `consumer` identity, UI actions are limited by that user's ACLs.

## Scripts

### `scripts/start-kafka.sh`

This script:

- writes `server.properties`
- configures Kafka in KRaft mode
- enables SCRAM on internal and external listeners
- formats storage if needed
- creates SCRAM users for `admin`, `publisher`, and `consumer`
- starts the Kafka broker

### `scripts/init-acls.sh`

This script:

- waits for Kafka to become ready
- authenticates as `admin`
- creates topic `sensor-data-1`
- applies ACLs for topic, group, and cluster access

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

Below is the **Kafka-related configuration** from a Python consumer with environment-based credentials and error handling (using `kafka-python`).

```python
from kafka import KafkaConsumer
import os

KAFKA_TOPIC = 'sensor-data-1'
KAFKA_BOOTSTRAP = 'localhost:9093'
KAFKA_USERNAME = os.getenv("KAFKA_CONSUMER_USERNAME", "consumer")
KAFKA_PASSWORD = os.getenv("KAFKA_CONSUMER_PASSWORD")

if not KAFKA_PASSWORD:
    raise RuntimeError("Missing environment variable: KAFKA_CONSUMER_PASSWORD")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=[KAFKA_BOOTSTRAP],
    group_id='my-group',
    client_id='consumer-1',
    auto_offset_reset='latest',
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanism='SCRAM-SHA-512',
    sasl_plain_username=KAFKA_USERNAME,
    sasl_plain_password=KAFKA_PASSWORD,
    api_version=(2, 6),
    max_poll_records=1,
)
```

### What this consumer needs

* Kafka bootstrap server: `localhost:9093`
* Username: `consumer` (or via `KAFKA_CONSUMER_USERNAME`)
* Password: set via `KAFKA_CONSUMER_PASSWORD`
* Topic permission: `READ` and `DESCRIBE` on `sensor-data-1`
* Group permission: `READ` on the consumer group
* Matching SASL mechanism (`SCRAM-SHA-512` in this case)

## Publisher example

Below is the **Kafka-related configuration** from a Python publisher with environment-based credentials, error handling, and a sensor1 payload structure (using `kafka-python`).

```python
from kafka import KafkaProducer
import json
import os

KAFKA_TOPIC = 'sensor-data-1'
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
KAFKA_USERNAME = os.getenv("KAFKA_PUBLISHER_USERNAME", "consumer")
KAFKA_PASSWORD = os.getenv("KAFKA_PUBLISHER_PASSWORD")

if not KAFKA_PASSWORD:
    raise RuntimeError("Missing environment variable: KAFKA_PUBLISHER_PASSWORD")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    security_protocol='SASL_PLAINTEXT',
    sasl_mechanism='SCRAM-SHA-512',
    sasl_plain_username=KAFKA_USERNAME,
    sasl_plain_password=KAFKA_PASSWORD,
    api_version=(2, 6),
    max_block_ms=5000,
)

# Example send
producer.send(KAFKA_TOPIC, {
    'sensor_id': 'sensor1',
    'extra_features': 'test',
    'properties': {
        'temperature': 273
    },
    'timestamp': 1710000000.0
})
```

### What this publisher needs

* Kafka bootstrap server: `localhost:9093`
* Username: set via `KAFKA_PUBLISHER_USERNAME`
* Password: `KAFKA_PUBLISHER_PASSWORD`
* Topic permission: `WRITE` and `DESCRIBE` on `sensor-data-1`
* Matching SASL mechanism (`SCRAM-SHA-512` in this case)

## Minimal Kafka-only examples

### Create topic `sensor-data-1`

```bash
cat <<EOF >/tmp/admin.properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="admin" password="admin-secret";
EOF

/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka:9092 \
  --command-config /tmp/admin.properties \
  --create \
  --topic sensor-data-1 \
  --partitions 1 \
  --replication-factor 1
```

### Produce messages to `sensor-data-1`

```bash
docker exec -it kafka bash
```

Then run:

```bash
cat <<EOF >/tmp/publisher.properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="publisher" password="${KAFKA_PUBLISHER_PASSWORD}";
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
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="consumer" password="${KAFKA_CONSUMER_PASSWORD}";
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

- `localhost:9093`
- `SASL_PLAINTEXT`
- `PLAIN`
- a valid username/password

### Python client issues

Common checks:

- `kafka-python` is installed
- your app is connecting to `localhost:9093` from the host, not `kafka:9092`
- the username matches the role you want to use
- the topic `sensor-data-1` exists
- ACLs have already been applied by `kafka-acl-init`
- the broker and client use the same SASL mechanism: `SCRAM-SHA-512`
- any dependent service like MQTT broker or AAS is reachable on the configured URL

