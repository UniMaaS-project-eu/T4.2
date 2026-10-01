#!/usr/bin/env python
# -*- coding: utf-8 -*-
from wotpy.wot.servient import Servient
from wotpy.wot.wot import WoT
from wotpy.protocols.http.client import HTTPClient
from wotpy.protocols.mqtt.client import MQTTClient
import datetime
import json
import pandas as pd
import pytz
import collections
import asyncio
from functools import partial

SECURITY_SCHEME_DICT = {
    "scheme": "oidc4vp"
}
CREDENTIALS_DICT = {
    "holder_url": "http://vo-holder:8085",
    "requester": "vo"
}

async def check():
    curr_vo_status = await vo_status(exposed_thing, 1)
    #curr_device_status = await device_status(exposed_thing, "http://192.168.49.1:9090", 1)

    print(f"Status VO: {curr_vo_status}")
    #print(f"Status Device: {curr_device_status}")

# This function should check if HVAC is turned on or off
async def read_property_from_device():
    http_client = HTTPClient()
    http_client.set_security(SECURITY_SCHEME_DICT, CREDENTIALS_DICT)
    wot = WoT(servient=Servient(clients=[http_client]))
    consumed_thing = await wot.consume_from_url("http://device:9090/device", credentials_dict=CREDENTIALS_DICT)
    temperature = await consumed_thing.read_property("temperature")
    print(f"Temperature: {temperature}")
