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


def propertyUpdated1_device1_on_next(data):
    print("Updated properties 1")

async def propertyUpdated1():
    wot = WoT(servient=Servient())
    # device1 =  await wot.consume_from_url('http://localhost:9090/device1')
    sensor1 =  await wot.consume_from_url('http://localhost:9090/sensor1')
    observedProperties1 = await sensor1.read_property('observedProperties1')
    await exposed_thing.properties['observedProperties1'].write(observedProperties1)
    print("Wrote properties 1")
#     device1.properties['observedProperties1'].subscribe(
#     on_next=lambda data: LOGGER.info(f'Value changed for an observable property: {data}'),
#     on_completed=LOGGER.info('Subscribed for an observable property: maintenanceNeeded'),
#     on_error=lambda error: LOGGER.info(f'Error for an observable property maintenanceNeeded: {error}')
# )

async def propertyUpdated2():
    wot = WoT(servient=Servient())
    sensor2 =  await wot.consume_from_url('http://localhost:9092/sensor2')
    observedProperties2 = await sensor2.read_property('observedProperties2')
    await exposed_thing.properties['observedProperties2'].write(observedProperties2)
    print("Wrote properties 2")

def propertyUpdated1_device1_on_completed():
    print("Completed 1")

def propertyUpdated1_device1_on_error(err):
    print(err)

def propertyUpdated2_device2_on_next(data):
    # wot = WoT(servient=Servient())
    # device2 = await wot.consume_from_url('http://localhost:9091/device2')
    # observedProperties2 =  device2.read_property('observedProperties2')
    # exposed_thing.properties['observedProperties2'].write(observedProperties2)
    print("Updated properties 2")

def propertyUpdated2_device2_on_completed():
    print("Completed 2")

def propertyUpdated2_device2_on_error(err):
    print(err)

