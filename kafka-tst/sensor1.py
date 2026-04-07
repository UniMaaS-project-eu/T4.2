import logging
import paho.mqtt.client as mqtt
import base64
import json
import time
import requests
import os

from kafka import KafkaProducer
from kafka.errors import KafkaError, TopicAuthorizationFailedError, NoBrokersAvailable, SaslAuthenticationFailedError, KafkaTimeoutError

logging.basicConfig()
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

# publish to Kafka topic using SASL_PLAINTEXT security protocols.
KAFKA_TOPIC = 'sensor-data-1'
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093") #if no env variable is set, it will use "localhost:9093" as default bootstrap servers
KAFKA_USERNAME = os.getenv("KAFKA_PUBLISHER_USERNAME", "publisher") #non existent to demonstrate different permissions
KAFKA_PASSWORD = os.getenv("KAFKA_PUBLISHER_PASSWORD")

if not KAFKA_PASSWORD:
    raise RuntimeError("Missing environment variable: KAFKA_PUBLISHER_PASSWORD")

observedproperty1_init = {
    'properties': {
        'temperature': 273
    }
}

def _init_producer():
    try:
        p = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            security_protocol='SASL_PLAINTEXT',
            sasl_mechanism='SCRAM-SHA-512',
            sasl_plain_username=KAFKA_USERNAME,
            sasl_plain_password=KAFKA_PASSWORD,
            api_version=(2, 6),
            max_block_ms=5000,
        )
        logging.getLogger('kafka').setLevel(logging.CRITICAL)
        p.partitions_for(KAFKA_TOPIC)
        return p
    except KafkaTimeoutError:
        LOGGER.error("Kafka authentication failed for user '%s'.", KAFKA_USERNAME)
    except NoBrokersAvailable:
        LOGGER.error("Kafka broker not reachable at '%s'.", KAFKA_BOOTSTRAP)
    except SaslAuthenticationFailedError:
        LOGGER.error("Kafka authentication failed for user '%s'.", KAFKA_USERNAME)
    except Exception as e:
        LOGGER.error("Failed to initialize Kafka producer: %s", e)
    finally:
        logging.getLogger('kafka').setLevel(logging.WARNING)
    return None

producer = _init_producer()


def on_send_error(excp):
    if isinstance(excp, TopicAuthorizationFailedError):
        LOGGER.error("Authorization failed: user '%s' cannot produce to topic '%s'.", KAFKA_USERNAME, KAFKA_TOPIC)
    elif isinstance(excp, (SaslAuthenticationFailedError, KafkaTimeoutError)):
        LOGGER.error("Authentication failed for user '%s'.", KAFKA_USERNAME)
    else:
        LOGGER.error("Failed to send message to Kafka: %s", excp)


def read_from_temperature_sensors():
    # Actual implementation of reading data from a sensor can go here
    # For the sake of example, let's just return a value
    print("HELLO")
    broker = "localhost"

    TOPIC_SENSOR1_DEVICE = f'device/sensor1/temperature'
    #port = mqtt["port"]
    readings = {"temperature": None}

    def on_message(client, userdata, msg):
        if msg.topic.endswith("temperature"):
            readings["temperature"] = msg.payload.decode()

    client = mqtt.Client()
    client.on_message = on_message
    client.connect(broker)
    client.subscribe([(TOPIC_SENSOR1_DEVICE, 0)])
    client.loop_start()

    # Wait max 1 second for messages
    t0 = time.time()
    while (readings["temperature"] is None) and (time.time() - t0 < 2):
        time.sleep(0.05)

    client.loop_stop()
    client.disconnect()
    print("Temperature:", readings["temperature"])

    return [readings["temperature"]]

async def update():
    # Read VO observed properties
    measuredResources = await exposed_thing.read_property('observedProperties1')
    if measuredResources is None:
        measuredResources = {"properties": {"temperature": None}}

    # Read sensor measurements
    sensor_measurements = read_from_temperature_sensors()
    property_names = list(measuredResources["properties"].keys())
    
    for i, prop in enumerate(property_names):
        measuredResources["properties"][prop] = float(sensor_measurements[i]) if sensor_measurements[i] is not None else 0.0

    # Update VO observedProperties
    await exposed_thing.properties['observedProperties1'].write(measuredResources)
    exposed_thing.emit_event("propertyUpdated1", "Property Updated")
    LOGGER.info("VO observedProperties1 updated: %s", measuredResources)

    if producer is None:
        LOGGER.warning("Kafka producer not initialized. Skipping publish.")
        return {'result': False, 'message': 'Kafka producer not initialized'}

    future = producer.send(KAFKA_TOPIC, {
        'sensor_id': 'sensor1',
        'extra_features': 'test',
        'properties': measuredResources["properties"],
        'timestamp': time.time()
    })
    future.add_errback(on_send_error)

    return {'result': True, 'message': 'All measurements have been updated!'}
