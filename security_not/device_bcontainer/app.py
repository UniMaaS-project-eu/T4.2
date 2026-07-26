import logging
import time
import asyncio

logging.basicConfig()
LOGGER = logging.getLogger()
LOGGER.setLevel(logging.INFO)

async def damage_simulation():
    # Simulate damage by emitting an event with a timestamp
    exposed_thing.emit_event('damageDetected', {
        'timestamp': "2026-06-07"
    })
    LOGGER.warning(f"damageDetected event emitted with timestamp  {time.strftime('%Y-%m-%d %H:%M:%S')}")

async def triggerLifecycleUpdate_handler(params):
    data = params["input"]
    exposed_thing.emit_event("lifecycleChanged", {
        "status": data.get("status"),
        "numberOfUses": data.get("numberOfUses"),
        "newLocation": data.get("newLocation"),
        "carbonFootprintIncrement": data.get("carbonFootprintIncrement", 0)
    })
    LOGGER.info(f"lifecycleChanged event emitted with data: {data}")
    return {"result": True, "message": "lifecycleChanged event emitted"}
