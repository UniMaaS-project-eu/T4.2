#!/usr/bin/env python3
import time
import random
import paho.mqtt.client as mqtt

BROKER = "localhost"  # host or "mosquitto" if inside the Docker network
PORT = 1883
# DEVICE1 = "1"
# DEVICE2 = "2"
# TOPIC_TEMP1 = f"device/{DEVICE1}/temperature"
# TOPIC_HUM1 = f"device/{DEVICE1}/humidity"
# TOPIC_TEMP2 = f"device/{DEVICE2}/temperature"
# TOPIC_HUM2 = f"device/{DEVICE2}/humidity"

TOPIC_SENSOR1_DEVICE = f'device/sensor1/temperature'
TOPIC_SENSOR2_DEVICE = f'device/sensor2/temperature'
TOPIC_SENSOR_RESOURCE = f'resource/sensor/density'
TOPIC_LOGISTICROUTE = f'logisticroute/congestion'

client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION1,
    client_id="publisher-1234"
)
client.connect(BROKER, PORT, 60)

try:
    print("Publishing simulated telemetry...")
    while True:
        # temp1 = round(random.uniform(20.0, 26.0), 2)
        # hum1 = round(random.uniform(35.0, 55.0), 2)
        # client.publish(TOPIC_TEMP1, str(temp1))
        # client.publish(TOPIC_HUM1, str(hum1))

        # temp2 = round(random.uniform(20.0, 26.0), 2)
        # hum2 = round(random.uniform(35.0, 55.0), 2)
        # client.publish(TOPIC_TEMP2, str(temp2))
        # client.publish(TOPIC_HUM2, str(hum2))
        
        # print(f"{TOPIC_TEMP1} -> {temp1}, {TOPIC_HUM1} -> {hum1}")
        # print(f"{TOPIC_TEMP2} -> {temp2}, {TOPIC_HUM2} -> {hum2}")

        temp1 = round(random.uniform(20.0, 26.0), 2)
        temp2 = round(random.uniform(20.0, 26.0), 2)
        density = round(random.uniform(0.0, 100.0), 2)
        congestion = round(random.uniform(0.0, 100.0), 2)

        client.publish(TOPIC_SENSOR1_DEVICE, str(temp1))
        client.publish(TOPIC_SENSOR2_DEVICE, str(temp2))
        client.publish(TOPIC_SENSOR_RESOURCE, str(density))
        client.publish(TOPIC_LOGISTICROUTE, str(congestion))
        
        print(f"{TOPIC_SENSOR1_DEVICE} -> {temp1}")
        print(f"{TOPIC_SENSOR2_DEVICE} -> {temp2}")
        print(f"{TOPIC_SENSOR_RESOURCE} -> {density}")
        print(f"{TOPIC_LOGISTICROUTE} -> {congestion}")

        time.sleep(1)
except KeyboardInterrupt:
    print("Stopped publishing")
finally:
    client.disconnect()
