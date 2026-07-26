import logging
import asyncio
import os
import time
import json

from kafka import KafkaProducer
from kafka.errors import (KafkaError, TopicAuthorizationFailedError, NoBrokersAvailable,
                          SaslAuthenticationFailedError, KafkaTimeoutError)

logging.basicConfig()
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)
FLEET = {
    "magnum": {"partNumber": 1114567, "aasId": "urn:aas:adient:magnum-optimum:1114567"},
    "europallet": {"partNumber": 2224567, "aasId": "urn:aas:adient:europallet:2224567"},
    "bcontainer": {"partNumber": 3334567, "aasId": "urn:aas:adient:b-container:3334567"},
}

REMOTE_PROP = "lifecycle"

# subscription bookkeeping for the watchdog.
# All start DEAD: the descriptor no longer requests propertyChanges, so the
# first sync_fleet tick performs the initial subscription for every VO
# (per-VO try/except -> a VO without observe forms just logs and is retried).
_sub_alive = {vo: False for vo in FLEET}
_sub_handles = {}


magnum_lifecycle_init = {
    "status": "manufactured",
    "condition": "new",
    "numberOfUses": 0,
    "currentLocation": "Plant Valencia",
    "lastMaintenanceDate": "",
    "lifecycleCarbonFootprint": 176.58,
}

europallet_lifecycle_init = {
    "status": "manufactured",
    "condition": "new",
    "numberOfUses": 0,
    "currentLocation": "Warehouse Madrid",
    "lastMaintenanceDate": "",
    "lifecycleCarbonFootprint": 7.75,
}

bcontainer_lifecycle_init = {
    "status": "manufactured",
    "condition": "new",
    "numberOfUses": 0,
    "currentLocation": "Plant Barcelona",
    "lastMaintenanceDate": "",
    "lifecycleCarbonFootprint": 427.9,
}

KAFKA_TOPIC = "cvo-fleet-updates"
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
KAFKA_USERNAME = os.getenv("KAFKA_PUBLISHER_USERNAME", "publisher")
KAFKA_PASSWORD = os.getenv("KAFKA_PUBLISHER_PASSWORD")

if not KAFKA_PASSWORD:
    raise RuntimeError("Missing environment variable: KAFKA_PUBLISHER_PASSWORD")


def _init_producer():
    try:
        p = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            security_protocol="SASL_PLAINTEXT",
            sasl_mechanism="SCRAM-SHA-512",
            sasl_plain_username=KAFKA_USERNAME,
            sasl_plain_password=KAFKA_PASSWORD,
            api_version=(2, 6),
            max_block_ms=5000,
        )
        logging.getLogger("kafka").setLevel(logging.CRITICAL)
        p.partitions_for(KAFKA_TOPIC)
        return p
    except KafkaTimeoutError:
        LOGGER.error("Kafka timeout while connecting to '%s'.", KAFKA_BOOTSTRAP)
    except NoBrokersAvailable:
        LOGGER.error("Kafka broker not reachable at '%s'.", KAFKA_BOOTSTRAP)
    except SaslAuthenticationFailedError:
        LOGGER.error("Kafka authentication failed for user '%s'.", KAFKA_USERNAME)
    except Exception as e:
        LOGGER.error("Failed to initialize Kafka producer: %s", e)
    finally:
        logging.getLogger("kafka").setLevel(logging.WARNING)
    return None


producer = _init_producer()


def on_send_error(excp):
    if isinstance(excp, TopicAuthorizationFailedError):
        LOGGER.error("Authorization failed: user '%s' cannot produce to topic '%s'.",
                     KAFKA_USERNAME, KAFKA_TOPIC)
    elif isinstance(excp, (SaslAuthenticationFailedError, KafkaTimeoutError)):
        LOGGER.error("Authentication failed for user '%s'.", KAFKA_USERNAME)
    else:
        LOGGER.error("Failed to send message to Kafka: %s", excp)


def publish_to_topic(vo_name, value):
    global producer
    if producer is None:
        producer = _init_producer()  # broker may not have been up at import time
        if producer is None:
            LOGGER.error("Kafka producer not available; skipping publish to '%s'", KAFKA_TOPIC)
            return
    meta = FLEET[vo_name]
    try:
        future = producer.send(KAFKA_TOPIC, {
            "source": "cvo",
            "vo": vo_name,
            "product_id": f"urn:ngsi-ld:Product:{meta['partNumber']}",
            "aas_id": meta["aasId"],
            "property": REMOTE_PROP,
            "data": value,
            "timestamp": time.time(),
        })
        future.add_errback(on_send_error)
    except Exception as e:
        LOGGER.error("Kafka publish failed for %s: %s", vo_name, e)


# ------------------------------------------------------------------
# Update path: mirror locally (-> InfluxDB), Kafka, aggregate event
# ------------------------------------------------------------------
def _extract_value(notification):
    """PropertyChangeEmittedEvent -> .data (PropertyChangeEventInit) -> .value.
    Falls back gracefully if a plain value is passed."""
    data = getattr(notification, "data", notification)
    return getattr(data, "value", data)


async def apply_update(vo_name, value):
    prop = f"{vo_name}_lifecycle"
    try:
        await exposed_thing.write_property(prop, value)  # persisted to Influx bucket <prop>
    except Exception as e:
        LOGGER.error("Failed to write local property %s: %s", prop, e)
        return
    publish_to_topic(vo_name, value)
    try:
        exposed_thing.emit_event("fleetLifecycleUpdated", {
            "vo": vo_name,
            "value": value,
            "timestamp": time.time(),
        })
    except Exception as e:
        LOGGER.error("Failed to emit fleetLifecycleUpdated: %s", e)
    LOGGER.info("cVO mirrored %s = %s", prop, value)


def _handle_change(vo_name, notification):
    """Shared, exception-proof body for all on_next handlers.
    Any exception escaping on_next would permanently kill the subscription."""
    try:
        value = _extract_value(notification)
        LOGGER.info("[%s] lifecycle changed: %s", vo_name, value)
        asyncio.create_task(apply_update(vo_name, value))
    except Exception as e:
        LOGGER.error("[%s] handler error (suppressed to keep subscription alive): %s",
                     vo_name, e)


def _mark_dead(vo_name, reason):
    _sub_alive[vo_name] = False
    LOGGER.error("[%s] lifecycle subscription DEAD (%s); watchdog will resubscribe",
                 vo_name, reason)


# ------------------------------------------------------------------
# Convention handlers picked up by the runtime for
# consumedVOs.<vo>.propertyChanges: [lifecycle]
# ------------------------------------------------------------------
def lifecycle_magnum_on_next(notification):
    _handle_change("magnum", notification)

def lifecycle_magnum_on_error(err):
    _mark_dead("magnum", err)

def lifecycle_magnum_on_completed():
    _mark_dead("magnum", "completed")


def lifecycle_europallet_on_next(notification):
    _handle_change("europallet", notification)

def lifecycle_europallet_on_error(err):
    _mark_dead("europallet", err)

def lifecycle_europallet_on_completed():
    _mark_dead("europallet", "completed")


def lifecycle_bcontainer_on_next(notification):
    _handle_change("bcontainer", notification)

def lifecycle_bcontainer_on_error(err):
    _mark_dead("bcontainer", err)

def lifecycle_bcontainer_on_completed():
    _mark_dead("bcontainer", "completed")


_ON_NEXT = {
    "magnum": lifecycle_magnum_on_next,
    "europallet": lifecycle_europallet_on_next,
    "bcontainer": lifecycle_bcontainer_on_next,
}
_ON_ERROR = {
    "magnum": lifecycle_magnum_on_error,
    "europallet": lifecycle_europallet_on_error,
    "bcontainer": lifecycle_bcontainer_on_error,
}
_ON_COMPLETED = {
    "magnum": lifecycle_magnum_on_completed,
    "europallet": lifecycle_europallet_on_completed,
    "bcontainer": lifecycle_bcontainer_on_completed,
}


def _resubscribe(vo_name, consumed):
    old = _sub_handles.pop(vo_name, None)
    if old is not None:
        try:
            old.dispose()
        except Exception:
            pass
    _sub_handles[vo_name] = consumed.properties[REMOTE_PROP].subscribe(
        on_next=_ON_NEXT[vo_name],
        on_error=_ON_ERROR[vo_name],
        on_completed=_ON_COMPLETED[vo_name],
    )
    _sub_alive[vo_name] = True
    LOGGER.info("[%s] (re)subscribed to remote property '%s'", vo_name, REMOTE_PROP)


# Periodic watchdog for the consumed VOs + polling fallback
async def sync_fleet():
    for vo_name in FLEET:
        consumed = consumed_vos.get(vo_name)
        if consumed is None:
            LOGGER.warning("Consumed VO '%s' not available", vo_name)
            continue

        # Watchdog: revive dead subscriptions
        if not _sub_alive.get(vo_name, False):
            try:
                _resubscribe(vo_name, consumed)
            except Exception as e:
                LOGGER.error("[%s] resubscribe failed: %s", vo_name, e)

        # Polling fallback / drift correction
        try:
            remote = await consumed.read_property(REMOTE_PROP)
        except Exception as e:
            LOGGER.warning("Could not read '%s' from %s: %s", REMOTE_PROP, vo_name, e)
            continue
        try:
            local = await exposed_thing.read_property(f"{vo_name}_lifecycle")
        except Exception:
            local = None
        if remote != local:
            LOGGER.info("[%s] drift detected via polling; mirroring", vo_name)
            await apply_update(vo_name, remote)