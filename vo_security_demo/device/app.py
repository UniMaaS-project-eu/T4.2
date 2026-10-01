temperature_init = 70
humidity_init = 60

async def read_temperature_property():
    global temperature_init
    await exposed_thing.properties["temperature"].write(temperature_init)
    print(temperature_init)
