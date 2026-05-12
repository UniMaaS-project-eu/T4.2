#!/usr/bin/env python
# -*- coding: utf-8 -*-
from wotpy.wot.servient import Servient
from wotpy.wot.wot import WoT
from wotpy.protocols.http.client import HTTPClient
import requests
import logging
import asyncio
import aiohttp

INF_YEARS = 999999


bridge_healthy = False
product_model_init = {}
registered_values_init = {}

PRODUCT_MODELS = {
    "magnum_optimum": {
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
    },
    "europallet": {
        "modelName": "Europallet",
        "description": "Wooden pallet with 1.2x1.08 meters",
        "usePurpose": "Transport raw materials and finished goods between manufacturing plants",
        "manufacturerName": "DS Smith Poland",
        "manufacturerAddress": "ul. Malików 150, 25-639 Kielce, Poland",
        "taricCode": "4415202000",
        "durabilityMinYears": INF_YEARS,
        "durabilityMaxYears": INF_YEARS,
        "repairability": "repairable",
        "manufacturingCarbonFootprint": 7.75
    },
    "b_container": {
        "modelName": "B-Container",
        "description": "Metal returnable and foldable container",
        "usePurpose": "Transport raw materials and finished goods between manufacturing plants",
        "manufacturerName": "STRUMET sp. z o.o.",
        "manufacturerAddress": "ul. Ks. Londzina 61, 43-246 Strumień, Poland",
        "taricCode": "7326901000",
        "durabilityMinYears": 5,
        "durabilityMaxYears": 15,
        "repairability": "repairable_with_spare_parts",
        "manufacturingCarbonFootprint": 427.9
    }
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


# Action handlers

async def registerProductInstance_handler(params):
    data = params["input"] or {}
    current_model = await exposed_thing.read_property("product_model")

    if current_model:
        return {
            "result": False,
            "message": "Product model is already initialized and cannot be changed"
        }

    model = PRODUCT_MODELS[data["modelType"]]

    registered_values = {
        "partNumber": data["partNumber"],
        "manufacturingDate": data["manufacturingDate"],
        "returnRatio": data["returnRatio"]
    }

    await exposed_thing.write_property("product_model", model)
    await exposed_thing.write_property("registered_values", registered_values)
    await exposed_thing.write_property("status", "manufactured")
    await exposed_thing.write_property("condition", "new")
    await exposed_thing.write_property("numberOfUses", 0)
    await exposed_thing.write_property("currentLocation", "Plant Valencia")
    await exposed_thing.write_property("lifecycleCarbonFootprint", model["manufacturingCarbonFootprint"])

    LOGGER.info("Registered new product instance: model=%s, partNumber=%s", data["modelType"], data["partNumber"])

    #create and save the json file with the product instance data
    product_jsonld = {
        "@context": {
            "schema": "https://schema.org/",
            "dpp": "https://example.org/dpp#"
        },
        "@type": "schema:Product",
        "@id": "urn:asset:container:1",

        "dpp:productModel": model,

        "dpp:registeredValues": {
            "partNumber": data["partNumber"],
            "manufacturingDate": data["manufacturingDate"],
            "returnRatio": data["returnRatio"]
        },

        "dpp:lifecycle": {
            "status": "manufactured",
            "condition": "new",
            "currentLocation": "Plant Valencia",
            "lastMaintenanceDate": None,
            "numberOfUses": 0,
            "lifecycleCarbonFootprint": {
                "value": model["manufacturingCarbonFootprint"],
                "unit": "kg CO2e"
            }
        }
    }
    # save_product_entity(product_jsonld)

    return {
        "result": True,
        "message": "Product instance registered"
    }


def save_product_entity(product_jsonld):
    import json
    import os

    output_dir = "product_instances"
    os.makedirs(output_dir, exist_ok=True)

    part_number = product_jsonld["dpp:registeredValues"]["partNumber"]
    filename = f"{output_dir}/container_{part_number}.jsonld"

    with open(filename, "w") as f:
        json.dump(product_jsonld, f, indent=2)

    LOGGER.info("Saved product entity to %s", filename)

async def getProductInfo_handler(params):
    return {
        "product_model": await exposed_thing.read_property("product_model"),
        "registered_values": await exposed_thing.read_property("registered_values"),
        "status": await exposed_thing.read_property("status"),
        "condition": await exposed_thing.read_property("condition"),
        "currentLocation": await exposed_thing.read_property("currentLocation"),
        "lastMaintenanceDate": await exposed_thing.read_property("lastMaintenanceDate"),
        "lifecycleCarbonFootprint": await exposed_thing.read_property("lifecycleCarbonFootprint"),
        "numberOfUses": await exposed_thing.read_property("numberOfUses")
    }

async def updateLocation_handler(params):
    new_location = params["input"].get("newLocation")
    if not new_location:
        return {
            "result": False,
            "message": "Missing newLocation in input"
        }

    await exposed_thing.write_property("currentLocation", new_location)
    LOGGER.info("Updated location: %s", new_location)

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

    await exposed_thing.write_property("status", new_status)
    LOGGER.info("Updated status: %s", new_status)

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

    await exposed_thing.write_property("condition", new_condition)
    LOGGER.info("Updated condition: %s", new_condition)

    return {
        "result": True,
        "message": f"Condition updated to {new_condition}"
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

    footprint = await exposed_thing.read_property('lifecycleCarbonFootprint')
    await exposed_thing.write_property('lastMaintenanceDate', maintenance_date)
    await exposed_thing.write_property('lifecycleCarbonFootprint', footprint + carbon_footprint_increment)
    await exposed_thing.write_property('status', 'maintenance')
    LOGGER.info("Performed maintenance: status=maintenance")

    exposed_thing.emit_event('maintenancePerformed', maintenance_date)

    return {
        "result": True,
        "message": "Maintenance performed, status set to maintenance"
    }

#consume damage events from device and update condition to damaged if damageDetected is received
def damageDetected_device_on_next(event):
    LOGGER.info("Damage event received: %s", event)
    asyncio.create_task(handle_damage_event(event))

async def handle_damage_event(event):
    data = event.data or {}
    timestamp = data.get("timestamp")
    await exposed_thing.write_property("condition", "damaged")
    exposed_thing.emit_event("conditionUpdated", "damaged")


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
    part_number = (await exposed_thing.read_property("registered_values")).get("partNumber")
    property_name = data.data.name
    value = data.data.value

    headers = {
        "Content-Type": "application/json", 
        "accept": "application/json"
    }
    payload = {
        "product_id": f"urn:ngsi-ld:Product:{part_number}",
        "property": property_name,
        "data": value
    }

    if bridge_healthy:
        try:
            async with aiohttp.request("POST", BASE_URL + "/update/property", json=payload, headers=headers) as response:
                LOGGER.info("Bridge [%d]: %s", response.status, await response.text())
                return response.status
        except aiohttp.ClientError as e:
            LOGGER.error("Bridge update failed: %s", e)
            return None



# Property change handlers - these can be used to trigger events or communicate with DPP when properties are updated
def status_on_next(data):
    LOGGER.info("Status property updated: %s", data)
    asyncio.create_task(send_request_to_bridge(data))

def condition_on_next(data):
    LOGGER.info("Condition property updated: %s", data)
    asyncio.create_task(send_request_to_bridge(data))

def numberOfUses_on_next(data):
    LOGGER.info("numberOfUses property updated: %s", data)
    asyncio.create_task(send_request_to_bridge(data))

def currentLocation_on_next(data):
    LOGGER.info("currentLocation property updated: %s", data)
    asyncio.create_task(send_request_to_bridge(data))

def lastMaintenanceDate_on_next(data):
    LOGGER.info("lastMaintenanceDate property updated: %s", data)
    asyncio.create_task(send_request_to_bridge(data))

def lifecycleCarbonFootprint_on_next(data):
    LOGGER.info("lifecycleCarbonFootprint property updated: %s", data)
    asyncio.create_task(send_request_to_bridge(data))