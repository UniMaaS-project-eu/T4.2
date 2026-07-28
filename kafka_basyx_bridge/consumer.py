#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Kafka -> BaSyx AAS sync service (UniMaaS ADIENT pilot).

Subscribes (as User:consumer, SCRAM-SHA-512) to the per-product VO topics
    magnum-product-<part>-updates
    europallet-product-<part>-updates
    b-container-product-<part>-updates
and to the cVO topic
    cvo-fleet-updates            (consumed for audit logging only)

For every "lifecycle" message from a product VO it:
  1. PATCHes the value of each element of the product's ProductLifecycle
     submodel in the BaSyx Submodel Repository (value-only PATCH, AAS v3 API).
  2. Resolves lifecycle.currentLocation against the SiteName matching key of
     the Site AAS instances (semanticId urn:unimaas:adient:aas:siteName) and
     maintains the siteHasResource object property
     (http://unimaas-project.eu/MSCOntology#siteHasResource):
       - ensures a SiteHasResource SubmodelElementCollection exists in the
         matched site's SiteData submodel,
       - adds a ReferenceElement Resource_<part> pointing to the product AAS,
       - removes that ReferenceElement from any other site that still holds it.

The service is idempotent and restart-safe: on startup it scans all SiteData
submodels to learn the current resource placement, and every operation is a
no-op if the repository is already in the desired state.
"""
import base64
import json
import logging
import os
import re
import time

import requests
from kafka import KafkaConsumer

logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("kafka-basyx-bridge")
LOGGER.setLevel(logging.INFO)
logging.getLogger("kafka").setLevel(logging.WARNING)

# ------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------
AAS_ENV = os.getenv("BASYX_AAS_ENV_URL", "http://aas-env:8081").rstrip("/")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9093")
KAFKA_USERNAME = os.getenv("KAFKA_CONSUMER_USERNAME", "consumer")
KAFKA_PASSWORD = os.getenv("KAFKA_CONSUMER_PASSWORD")
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "basyx-aas-sync")
TOPIC_PATTERN = os.getenv(
    "KAFKA_TOPIC_PATTERN",
    r"^(magnum-product-.*-updates|europallet-product-.*-updates|b-container-product-.*-updates|cvo-.*)$",
)

if not KAFKA_PASSWORD:
    raise RuntimeError("Missing environment variable: KAFKA_CONSUMER_PASSWORD")

# partNumber -> AAS ids (aligned with the *_dpp_aas.json files loaded in BaSyx)
PRODUCTS = {
    "1114567": {
        "aasId": "urn:unimaas:adient:resource:magnum_optimum_1114567",
        "lifecycleSubmodelId": "urn:aas:adient:resource:magnum_optimum_1114567:sm:lifecycle",
    },
    "2224567": {
        "aasId": "urn:unimaas:adient:resource:europallet_2224567",
        "lifecycleSubmodelId": "urn:aas:adient:resource:europallet_2224567:sm:lifecycle",
    },
    "3334567": {
        "aasId": "urn:unimaas:adient:resource:b_container_3334567",
        "lifecycleSubmodelId": "urn:aas:adient:resource:b_container_3334567:sm:lifecycle",
    },
}

# VO lifecycle fields -> idShorts inside the ProductLifecycle submodel
LIFECYCLE_ELEMENTS = [
    "status",
    "condition",
    "currentLocation",
    "lastMaintenanceDate",
    "lifecycleCarbonFootprint",
    "numberOfUses",
]

MSC = "http://unimaas-project.eu/MSCOntology#"
SITE_NAME_SEMANTIC_ID = "urn:unimaas:adient:aas:siteName"
SITE_HAS_RESOURCE_SEMANTIC_ID = MSC + "siteHasResource"

# Optional alias map for VO locations that do not literally equal a SiteName
# matching key, e.g. '{"Plant Valencia": "site_14", "Plant Barcelona": "site_33"}'
LOCATION_ALIASES = json.loads(os.getenv("LOCATION_ALIASES", "{}"))


# ------------------------------------------------------------------
# BaSyx AAS v3 REST helpers
# ------------------------------------------------------------------
def b64id(identifier: str) -> str:
    """Base64url-encode an AAS/Submodel identifier (no padding), per AAS Part 2."""
    return base64.urlsafe_b64encode(identifier.encode()).decode().rstrip("=")


def sm_url(submodel_id: str, path: str = "") -> str:
    url = f"{AAS_ENV}/submodels/{b64id(submodel_id)}"
    if path:
        url += f"/submodel-elements/{path}"
    return url


# FIXED CODE
def patch_element_value(submodel_id: str, id_short_path: str, value) -> bool:
    """Value-only PATCH of a Property. BaSyx requires numeric types as JSON strings."""
    url = sm_url(submodel_id, id_short_path) + "/$value"
    
    if isinstance(value, (int, float)):
        candidate = str(value)
    else:
        candidate = value
    
    try:
        r = requests.patch(url, json=candidate, timeout=10)
    except requests.RequestException as e:
        LOGGER.error("PATCH %s failed: %s", url, e)
        return False
    
    if r.status_code in (200, 204):
        LOGGER.debug("PATCH %s ← %s succeeded", url, r.status_code)
        return True
    
    LOGGER.error("PATCH %s -> %s: %s", url, r.status_code, r.text[:200])
    return False


def get_element(submodel_id: str, id_short_path: str):
    try:
        r = requests.get(sm_url(submodel_id, id_short_path), timeout=10)
    except requests.RequestException as e:
        LOGGER.error("GET element failed: %s", e)
        return None
    return r.json() if r.status_code == 200 else None


def post_element(submodel_id: str, element: dict, parent_path: str = "") -> bool:
    url = sm_url(submodel_id, parent_path) if parent_path else f"{AAS_ENV}/submodels/{b64id(submodel_id)}/submodel-elements"
    try:
        r = requests.post(url, json=element, timeout=10)
    except requests.RequestException as e:
        LOGGER.error("POST %s failed: %s", url, e)
        return False
    if r.status_code in (200, 201):
        return True
    LOGGER.error("POST %s -> %s %s", url, r.status_code, r.text[:200])
    return False


def delete_element(submodel_id: str, id_short_path: str) -> bool:
    url = sm_url(submodel_id, id_short_path)
    try:
        r = requests.delete(url, timeout=10)
    except requests.RequestException as e:
        LOGGER.error("DELETE %s failed: %s", url, e)
        return False
    return r.status_code in (200, 204)


def list_submodels():
    """Paginated listing of every submodel in the repository."""
    submodels, cursor = [], None
    while True:
        params = {"limit": 100}
        if cursor:
            params["cursor"] = cursor
        try:
            r = requests.get(f"{AAS_ENV}/submodels", params=params, timeout=15)
            r.raise_for_status()
        except requests.RequestException as e:
            LOGGER.error("Listing submodels failed: %s", e)
            return submodels
        body = r.json()
        submodels.extend(body.get("result", []))
        cursor = (body.get("paging_metadata") or {}).get("cursor")
        if not cursor:
            return submodels


# ------------------------------------------------------------------
# Site index and siteHasResource maintenance
# ------------------------------------------------------------------
class SiteIndex:
    """siteName (matching key) -> SiteData submodel id, plus the current
    placement of every resource, learned by scanning SiteHasResource."""

    def __init__(self):
        self.site_name_to_sm = {}
        self.resource_site = {}  # partNumber -> siteName

    def refresh(self):
        self.site_name_to_sm.clear()
        placements = {}
        for sm in list_submodels():
            if sm.get("idShort") != "SiteData":
                continue
            sm_id = sm["id"]
            site_name = None
            for el in sm.get("submodelElements", []):
                if el.get("idShort") == "SiteName":
                    site_name = el.get("value")
                elif el.get("idShort") == "SiteHasResource":
                    for ref in el.get("value", []):
                        m = re.match(r"^Resource_(\d+)$", ref.get("idShort", ""))
                        if m:
                            placements[m.group(1)] = sm_id
            if site_name:
                self.site_name_to_sm[site_name] = sm_id
        # translate submodel ids back to site names
        sm_to_name = {v: k for k, v in self.site_name_to_sm.items()}
        self.resource_site = {part: sm_to_name.get(sm_id) for part, sm_id in placements.items()}
        LOGGER.info("Site index: %d sites %s; placements: %s",
                    len(self.site_name_to_sm), sorted(self.site_name_to_sm), self.resource_site)


SITES = SiteIndex()


def ensure_site_has_resource_collection(site_sm_id: str) -> bool:
    if get_element(site_sm_id, "SiteHasResource") is not None:
        return True
    collection = {
        "modelType": "SubmodelElementCollection",
        "idShort": "SiteHasResource",
        "semanticId": {
            "type": "ExternalReference",
            "keys": [{"type": "GlobalReference", "value": SITE_HAS_RESOURCE_SEMANTIC_ID}],
        },
        "description": [{
            "language": "en",
            "text": "Resources (returnable containers) currently located at this Site (sc:siteHasResource)",
        }],
        "value": [],
    }
    return post_element(site_sm_id, collection)


def reference_element(part_number: str, aas_id: str) -> dict:
    return {
        "modelType": "ReferenceElement",
        "idShort": f"Resource_{part_number}",
        "semanticId": {
            "type": "ExternalReference",
            "keys": [{"type": "GlobalReference", "value": SITE_HAS_RESOURCE_SEMANTIC_ID}],
        },
        "description": [{
            "language": "en",
            "text": f"Resource {part_number} is currently located at this Site",
        }],
        "value": {
            "type": "ModelReference",
            "keys": [{"type": "AssetAdministrationShell", "value": aas_id}],
        },
    }


def place_resource_at_site(part_number: str, aas_id: str, location: str):
    """Idempotently link the product AAS to the site matching `location` and
    unlink it from every other site."""
    site_name = LOCATION_ALIASES.get(location, location)
    target_sm = SITES.site_name_to_sm.get(site_name)
    if target_sm is None:
        # index may be stale (site AAS added after startup)
        SITES.refresh()
        target_sm = SITES.site_name_to_sm.get(site_name)
    if target_sm is None:
        LOGGER.warning("currentLocation '%s' does not match any Site SiteName key; "
                       "siteHasResource not updated (known sites: %s)",
                       location, sorted(SITES.site_name_to_sm))
        return

    element_path = f"SiteHasResource.Resource_{part_number}"

    # 1. remove from the previous site (if different)
    previous = SITES.resource_site.get(part_number)
    if previous and previous != site_name:
        prev_sm = SITES.site_name_to_sm.get(previous)
        if prev_sm and delete_element(prev_sm, element_path):
            LOGGER.info("Unlinked resource %s from site %s", part_number, previous)

    # 2. add to the target site (if not already there)
    if get_element(target_sm, element_path) is None:
        if not ensure_site_has_resource_collection(target_sm):
            return
        if post_element(target_sm, reference_element(part_number, aas_id), "SiteHasResource"):
            LOGGER.info("Linked resource %s to site %s (siteHasResource)", part_number, site_name)
    SITES.resource_site[part_number] = site_name


# ------------------------------------------------------------------
# Message handling
# ------------------------------------------------------------------
def handle_lifecycle_message(msg: dict):
    product_id = msg.get("product_id", "")
    m = re.match(r"^urn:ngsi-ld:Product:(\d+)$", product_id)
    if not m:
        LOGGER.warning("Unrecognized product_id '%s'; skipping", product_id)
        return
    part_number = m.group(1)
    product = PRODUCTS.get(part_number)
    if product is None:
        LOGGER.warning("No AAS mapping for partNumber %s; skipping", part_number)
        return

    lifecycle = msg.get("data") or {}
    sm_id = product["lifecycleSubmodelId"]

    for id_short in LIFECYCLE_ELEMENTS:
        if id_short not in lifecycle:
            continue
        value = lifecycle[id_short]
        if value is None:
            LOGGER.debug("Skipping %s (null value in message)", id_short)
            continue
            # value = ""
        if patch_element_value(sm_id, id_short, value):
            LOGGER.info("AAS %s: %s = %s", part_number, id_short, value)

    location = lifecycle.get("currentLocation")
    if location:
        place_resource_at_site(part_number, product["aasId"], location)


def main():
    LOGGER.info("Waiting for BaSyx AAS environment at %s ...", AAS_ENV)
    while True:
        try:
            if requests.get(f"{AAS_ENV}/submodels", params={"limit": 1}, timeout=5).status_code == 200:
                break
        except requests.RequestException:
            pass
        time.sleep(5)
    LOGGER.info("BaSyx AAS environment is up")

    SITES.refresh()

    consumer = KafkaConsumer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=KAFKA_GROUP_ID,
        security_protocol="SASL_PLAINTEXT",
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=KAFKA_USERNAME,
        sasl_plain_password=KAFKA_PASSWORD,
        api_version=(2, 6),
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        metadata_max_age_ms=30000,  # picks up newly auto-created VO topics
    )
    consumer.subscribe(pattern=TOPIC_PATTERN)
    LOGGER.info("Subscribed to pattern %s as group '%s'", TOPIC_PATTERN, KAFKA_GROUP_ID)

    for record in consumer:
        try:
            msg = record.value
            if record.topic.startswith("cvo-"):
                LOGGER.info("[cvo audit] %s", msg)  # cVO stream: audit only
                continue
            if msg.get("property") == "lifecycle":
                handle_lifecycle_message(msg)
            else:
                LOGGER.debug("Ignoring property '%s' on %s", msg.get("property"), record.topic)
        except Exception:
            LOGGER.exception("Failed to process record from %s", record.topic)


if __name__ == "__main__":
    main()
