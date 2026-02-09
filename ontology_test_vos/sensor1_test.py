import json
import logging
# import math
# import time
import paho.mqtt.subscribe as subscribe
import paho.mqtt.client as mqtt
import random
import influxdb_client, os, time
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from wotpy2.wot.servient import Servient
from wotpy2.wot.wot import WoT
import smtplib, ssl
import requests
import base64

logging.basicConfig()
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

observedproperties1_init = {
    'properties': {
        'temperature': 0
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
    # DEVICE = "1"
    # TOPIC_TEMP = f"device/{DEVICE}/temperature"
    # TOPIC_HUM = f"device/{DEVICE}/humidity"

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
    # print("Humidity:", readings["humidity"])

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

    # --- Update corresponding AAS properties ---
    aas_base_url = "http://localhost:8081"  
    submodel_id = to_base64("urn:aas:submodel:property1:test") 

    for prop in property_names:
        payload = {
            "modelType": "Property",
            "value": str(measuredResources["properties"][prop]),
            "valueType": "xs:double",
            "idShort": prop # match your AAS property ID ("temperature", "humidity")
        }
        try:
            response = requests.put(
                f"{aas_base_url}/submodels/{submodel_id}/submodel-elements/{prop}",
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload)
            )
            if response.status_code == 200 or response.status_code == 204:
                print(f"AAS property '{prop}' updated successfully:", response.json())
            else:
                print(f"Failed to update AAS property '{prop}'. Status: {response.status_code}, Response: {response.text}")
        except Exception as e:
            print(f"Error updating AAS property '{prop}':", e)

    return {'result': True, 'message': 'All measurements have been updated!'}

async def update_handler(params):
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

    # --- Update corresponding AAS properties ---
    aas_base_url = "http://localhost:8081"  
    submodel_id = to_base64("urn:aas:submodel:property1:test") 

    for prop in property_names:
        payload = {
            "modelType": "Property",
            "value": str(measuredResources["properties"][prop]),
            "valueType": "xs:double",
            "idShort": prop # match your AAS property ID ("temperature", "humidity")
        }
        try:
            response = requests.put(
                f"{aas_base_url}/submodels/{submodel_id}/submodel-elements/{prop}",
                headers={"Content-Type": "application/json"},
                data=json.dumps(payload)
            )
            if response.status_code == 200 or response.status_code == 204:
                print(f"AAS property '{prop}' updated successfully:", response.json())
            else:
                print(f"Failed to update AAS property '{prop}'. Status: {response.status_code}, Response: {response.text}")
        except Exception as e:
            print(f"Error updating AAS property '{prop}':", e)

    return {'result': True, 'message': 'All measurements have been updated!'}
