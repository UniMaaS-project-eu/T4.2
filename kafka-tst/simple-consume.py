from kafka import KafkaConsumer
import time
import json
import requests
import base64
import os
import logging
from kafka.errors import KafkaError, TopicAuthorizationFailedError, NoBrokersAvailable, SaslAuthenticationFailedError, KafkaTimeoutError



logging.basicConfig()
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

def to_base64(text: str) -> str:
    bytes_data = text.encode("utf-8")
    b64_bytes = base64.b64encode(bytes_data)
    return b64_bytes.decode("utf-8")



BASE_URL = "http://localhost:8081/"
SUBMODEL_ID = to_base64("urn:aas:submodel:characteristic1:test") # it can be passed as env variable or read from a config file, hardcoding here for simplicity
AAS_URL = f"{BASE_URL}/submodels/{SUBMODEL_ID}/submodel-elements/temperature"

# consume from Kafka topic using SASL_PLAINTEXT security protocols
KAFKA_TOPIC = 'sensor-data-1'
KAFKA_BOOTSTRAP = 'localhost:9093' # host machine, not docker network
KAFKA_USERNAME = os.getenv("KAFKA_CONSUMER_USERNAME", "consumer") #if no env variable is set, it will use "consumer" as default username
KAFKA_PASSWORD = os.getenv("KAFKA_CONSUMER_PASSWORD")
if not KAFKA_PASSWORD:
    raise RuntimeError("Missing environment variable: KAFKA_CONSUMER_PASSWORD")

def _init_consumer():
    try:
        c = KafkaConsumer(
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
        logging.getLogger('kafka').setLevel(logging.CRITICAL)
        c.partitions_for_topic(KAFKA_TOPIC)  # eagerly triggers auth/permission check
        return c
    except KafkaTimeoutError:
        LOGGER.error("Kafka timeout during init — authentication likely failed for user '%s'.", KAFKA_USERNAME)
    except NoBrokersAvailable:
        LOGGER.error("Kafka broker not reachable at '%s'.", KAFKA_BOOTSTRAP)
    except SaslAuthenticationFailedError:
        LOGGER.error("Kafka authentication failed for user '%s'.", KAFKA_USERNAME)
    except Exception as e:
        LOGGER.error("Failed to initialize Kafka consumer: %s", e)
    finally:
        logging.getLogger('kafka').setLevel(logging.WARNING)
    return None


consumer = _init_consumer()

if consumer is None:
    LOGGER.error("Kafka consumer not initialized. Exiting.")
    exit(1)

## consume messages from Kafka and forward to AAS
for msg in consumer:
    LOGGER.info("Consumed from topic '%s': %s", msg.topic, msg.value)
    try:
        tst = json.loads(msg.value.decode('utf-8'))
        temperature = tst["properties"]["temperature"]
        LOGGER.info("Temperature: %s", temperature)
    except (json.JSONDecodeError, KeyError) as e:
        LOGGER.error("Failed to parse message: %s", e)
        continue

    LOGGER.info("Forwarding to AAS...")
    payload = {
        "modelType": "Property",
        "value": str(temperature),
        "valueType": "xs:double",
        "idShort": "temperature"
    }
    try:
        response = requests.put(AAS_URL, headers={"Content-Type": "application/json"}, data=json.dumps(payload))
        LOGGER.info("AAS response: %s - %s", response.status_code, response.text)
    except requests.RequestException as e:
        LOGGER.error("Failed to forward to AAS: %s", e)

    time.sleep(1)