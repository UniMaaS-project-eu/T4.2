import logging
import paho.mqtt.client as mqtt
import base64
import json
import time
import requests

from kafka import KafkaProducer

#simple publishing logic to Kafka topic, without considering security for simplicity. In production, you should consider using SSL/TLS and authentication mechanisms.
KAFKA_TOPIC = 'sensor-data'
producer = KafkaProducer(
    bootstrap_servers='localhost:9093', #currently using PLAINTEXT_HOST listener for external access
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

logging.basicConfig()
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

observedproperties1_init = {
    'properties': {
        'temperature': 273
    }
}   

def to_base64(text: str) -> str:
    bytes_data = text.encode("utf-8")
    b64_bytes = base64.b64encode(bytes_data)
    return b64_bytes.decode("utf-8")

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
    print("VO observedProperties1 updated:", measuredResources)

    producer.send(KAFKA_TOPIC, {
    'sensor_id': 'sensor1',
    'extra_features': 'test',
    'properties': measuredResources["properties"],
    'timestamp': time.time()
    })
    producer.flush()

    return {'result': True, 'message': 'All measurements have been updated!'}
