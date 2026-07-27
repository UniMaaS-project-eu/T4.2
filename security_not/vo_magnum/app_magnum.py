#!/usr/bin/env python
# -*- coding: utf-8 -*-
from wotpy.wot.servient import Servient
from wotpy.wot.wot import WoT
from wotpy.protocols.http.client import HTTPClient
# import requests
import logging
import asyncio
import aiohttp
import os
import time
import json

from kafka import KafkaProducer
from kafka.errors import KafkaError, TopicAuthorizationFailedError, NoBrokersAvailable, SaslAuthenticationFailedError, KafkaTimeoutError

bridge_healthy = False

# initialize properties with default values
product_model_init = {
    "modelName": "Magnum Optimum",
    "description": "Plastic returnable container",
    "usePurpose": "Transport raw materials and finished goods between manufacturing plants",
    "manufacturerName": "Schoeller Allibert GmbH",
    "manufacturerAddress": "Sacktannen 30, 19057 Schwerin, Germany",
    "taricCode": "3923109000",
    "durabilityMinYears": 5,
    "durabilityMaxYears": 15,
    "repairability": "repairable_with_spare_parts",
    "manufacturingCarbonFootprint": 176.58
}

registered_values_init = {
    "partNumber": 1114567,
    "manufacturingDate": "2025-01-01",
    "returnRatio": 3
}

lifecycle_init = {
    "status": "manufactured",
    "condition": "new",
    "numberOfUses": 0,
    "currentLocation": "Plant Valencia",
    "lastMaintenanceDate": None,
    "lifecycleCarbonFootprint": product_model_init["manufacturingCarbonFootprint"]
}

VALID_STATUSES = {
    "manufactured",
    "in_use",
    "maintenance",
    "repaired",
    "retired",
    "recycled"
}

VALID_CONDITIONS = {
    "new",
    "good",
    "worn",
    "damaged",
    "repaired"
}

# Base URL for bridge communication
BASE_URL = "https://unimaas.odins.es/dpp/vo-wot-ngsi-ld"

logging.basicConfig()
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)


# publish to Kafka topic using SASL_PLAINTEXT security protocols.
KAFKA_TOPIC = f'magnum-product-{registered_values_init["partNumber"]}-updates'
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093") #if no env variable is set, it will use "localhost:9093" as default bootstrap servers
KAFKA_USERNAME = os.getenv("KAFKA_PUBLISHER_USERNAME", "publisher")
KAFKA_PASSWORD = os.getenv("KAFKA_PUBLISHER_PASSWORD")

if not KAFKA_PASSWORD:
    raise RuntimeError("Missing environment variable: KAFKA_PUBLISHER_PASSWORD")

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

# Action handlers
async def getProductInfo_handler(params):
    return {
        "product_model": await exposed_thing.read_property("product_model"),
        "registered_values": await exposed_thing.read_property("registered_values"),
        "lifecycle": await exposed_thing.read_property("lifecycle")
    }

async def updateLocation_handler(params):
    new_location = params["input"].get("newLocation")
    if not new_location:
        return {
            "result": False,
            "message": "Missing newLocation in input"
        }
    current = await exposed_thing.read_property("lifecycle")
    current["currentLocation"] = new_location
    await exposed_thing.write_property("lifecycle", current)
    LOGGER.info("Updated location: %s", new_location)
    exposed_thing.emit_event("locationUpdated", new_location)

    return {
        "result": True,
        "message": f"Location updated to {new_location}"
    }


async def updateStatus_handler(params):
    new_status = params["input"].get("status")
    if not new_status:
        return {
            "result": False,
            "message": "Missing status in input"
        }

    if new_status not in VALID_STATUSES:
        return {
            "result": False,
            "message": f"Invalid status '{new_status}'. Allowed values are: {sorted(VALID_STATUSES)}"
        }

    current = await exposed_thing.read_property("lifecycle")
    current["status"] = new_status
    await exposed_thing.write_property("lifecycle", current)
    LOGGER.info("Updated status: %s", new_status)
    exposed_thing.emit_event("statusUpdated", new_status)

    return {
        "result": True,
        "message": f"Status updated to {new_status}"
    }

async def updateCondition_handler(params):
    new_condition = params["input"].get("condition")
    if not new_condition:
        return {
            "result": False,
            "message": "Missing condition in input"
        }

    if new_condition not in VALID_CONDITIONS:
        return {
            "result": False,
            "message": f"Invalid condition '{new_condition}'. Allowed values are: {sorted(VALID_CONDITIONS)}"
        }

    current = await exposed_thing.read_property("lifecycle")
    current["condition"] = new_condition
    await exposed_thing.write_property("lifecycle", current)
    LOGGER.info("Updated condition: %s", new_condition)
    exposed_thing.emit_event("conditionUpdated", new_condition)

    return {
        "result": True,
        "message": f"Condition updated to {new_condition}"
    }

async def updateNumberOfUses_handler(params):
    new_number_of_uses = params["input"].get("numberOfUses")
    if new_number_of_uses is None:
        return {
            "result": False,
            "message": "Missing numberOfUses in input"
        }

    if not isinstance(new_number_of_uses, int) or new_number_of_uses < 0:
        return {
            "result": False,
            "message": "numberOfUses must be a non-negative integer"
        }

    current = await exposed_thing.read_property("lifecycle")
    current["numberOfUses"] = new_number_of_uses
    await exposed_thing.write_property("lifecycle", current)
    LOGGER.info("Updated numberOfUses: %d", new_number_of_uses)
    exposed_thing.emit_event("numberOfUsesUpdated", new_number_of_uses)

    return {
        "result": True,
        "message": f"numberOfUses updated to {new_number_of_uses}"
    }
    
async def performMaintenance_handler(params):
    param_data = params["input"] or {}
    maintenance_date = param_data.get("date")
    carbon_footprint_increment = param_data.get("lifecycleCarbonFootprintIncrement", 0)

    if not maintenance_date:
        return {
            "result": False,
            "message": "Missing date in input"
        }
    current = await exposed_thing.read_property("lifecycle")
    updated_footprint = current.get("lifecycleCarbonFootprint", 0) + carbon_footprint_increment
    current["lastMaintenanceDate"] = maintenance_date
    current["lifecycleCarbonFootprint"] = updated_footprint
    current["status"] = "maintenance"
    await exposed_thing.write_property("lifecycle", current)
    LOGGER.info("Performed maintenance: status=maintenance")

    exposed_thing.emit_event('maintenancePerformed', maintenance_date)

    return {
        "result": True,
        "message": "Maintenance performed, status set to maintenance"
    }

# Event handlers for device events
def damageDetected_device_on_next(event):
    LOGGER.info("Damage event received: %s", event)
    asyncio.create_task(handle_damage_event(event))

async def handle_damage_event(event):
    data = event.data or {}
    timestamp = data.get("timestamp")
    current = await exposed_thing.read_property("lifecycle")
    current["condition"] = "damaged"
    await exposed_thing.write_property("lifecycle", current)
    exposed_thing.emit_event("conditionUpdated", "damaged")

def lifecycleChanged_device_on_next(event):
    LOGGER.info("Lifecycle change event received: %s", event)
    asyncio.create_task(handle_lifecycle_change_event(event))

async def handle_lifecycle_change_event(event):
    """
    FIXED VERSION: Only update lifecycle fields that are actually present
    in the device event. Do NOT overwrite with None.
    
    This prevents a partial device event from blanking out fields that were
    not included in that event.
    """
    data = event.data or {}
    
    # Read current lifecycle state
    current = await exposed_thing.read_property("lifecycle")
    
    # ✅ Mapping of device event keys to lifecycle property names.
    # Only keys present in this mapping will be updated.
    field_mapping = {
        "status": "status",
        "numberOfUses": "numberOfUses",
        "newLocation": "currentLocation",
        "condition": "condition"
    }
    
    updated_fields = []  # Track which fields were actually updated
    
    # ✅ Only update fields that are present AND non-None in the event
    for event_key, lifecycle_key in field_mapping.items():
        if event_key in data and data[event_key] is not None:
            current[lifecycle_key] = data[event_key]
            updated_fields.append((lifecycle_key, data[event_key]))
            LOGGER.info("Lifecycle updated from device event: %s = %s", 
                       lifecycle_key, data[event_key])
    
    # ✅ Handle carbon footprint increment (optional, default 0)
    if "carbonFootprintIncrement" in data:
        increment = data.get("carbonFootprintIncrement", 0)
        if isinstance(increment, (int, float)) and increment != 0:
            updated_footprint = current.get("lifecycleCarbonFootprint", 0) + increment
            current["lifecycleCarbonFootprint"] = updated_footprint
            updated_fields.append(("lifecycleCarbonFootprint", updated_footprint))
            LOGGER.info("Carbon footprint incremented: %s", updated_footprint)
    
    # ✅ Write the updated lifecycle back to the VO (only if something changed)
    if updated_fields:
        await exposed_thing.write_property("lifecycle", current)
        LOGGER.info("Lifecycle written after device event: %s", updated_fields)
        
        # ✅ Emit events only for fields that were actually updated
        for field_name, new_value in updated_fields:
            if field_name == "status":
                exposed_thing.emit_event("statusUpdated", new_value)
            elif field_name == "numberOfUses":
                exposed_thing.emit_event("numberOfUsesUpdated", new_value)
            elif field_name == "currentLocation":
                exposed_thing.emit_event("locationUpdated", new_value)
            elif field_name == "condition":
                exposed_thing.emit_event("conditionUpdated", new_value)
            elif field_name == "lifecycleCarbonFootprint":
                exposed_thing.emit_event("lifecycleCarbonFootprintUpdated", new_value)
    else:
        LOGGER.info("Device event contained no recognized/non-null fields; lifecycle unchanged")

# Helper functions for bridge communication

# check bridge health periodically
async def check_bridge_health():
    global bridge_healthy
    try:
        async with aiohttp.request("GET", BASE_URL + "/health") as response:
            if response.status == 200:
                health = await response.json()
                LOGGER.info("Bridge health: %s", health)
                if health.get("status") == "ok":
                    LOGGER.info("Bridge is healthy")
                    bridge_healthy = True
            else:
                LOGGER.error("Bridge health check failed with status: %d", response.status)
                bridge_healthy = False

    except aiohttp.ClientError as e:
        LOGGER.error("Bridge health check error: %s", e)
        bridge_healthy = False

#actual function to send property updates to the bridge
async def send_request_to_bridge(data):
    LOGGER.info("Preparing to send update to bridge for property: %s", data.data.name)
    # part_number = (await exposed_thing.read_property("registered_values")).get("partNumber")
    part_number = registered_values_init.get("partNumber")  # Use the initial part number for bridge update
    property_name = data.data.name
    value = data.data.value

    headers = {
        "Content-Type": "application/json", 
        "accept": "application/json"
    }
    payload = {
        "product_id": f"urn:ngsi-ld:Product:{part_number}",
        "property": property_name,
        "data": value,
        "timestamp": time.time()
    }

    if bridge_healthy:
        try:
            async with aiohttp.request("POST", BASE_URL + "/update/property", json=payload, headers=headers) as response:
                LOGGER.info("Bridge [%d]: %s", response.status, await response.text())
                return response.status
        except aiohttp.ClientError as e:
            LOGGER.error("Bridge update failed: %s", e)
            return None

async def publish_to_topic(property_name, property_value):
    part_number = registered_values_init.get("partNumber")  # Use the initial part number for Kafka message
    future = producer.send(KAFKA_TOPIC, {
        "product_id": f"urn:ngsi-ld:Product:{part_number}",
        "property": property_name,
        "data": property_value,
        'timestamp': time.time()
    })
    future.add_errback(on_send_error)

# Property change handlers - these can be used to trigger events or communicate with DPP when properties are updated
def lifecycle_on_next(data):
    # LOGGER.info("lifecycle property updated: %s", data)
    LOGGER.info("lifecycle changed, partial write was: %s", data.data.value)
    # asyncio.create_task(publish_to_topic("lifecycle", data.data.value))
    asyncio.create_task(publish_full_lifecycle())

async def publish_full_lifecycle():
    lifecycle = await exposed_thing.read_property("lifecycle")
    asyncio.create_task(publish_to_topic("lifecycle", lifecycle))
    asyncio.create_task(send_request_to_bridge("lifecycle", lifecycle))