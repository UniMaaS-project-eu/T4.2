from kafka import KafkaConsumer
import time
import json
import requests
import base64


def to_base64(text: str) -> str:
    bytes_data = text.encode("utf-8")
    b64_bytes = base64.b64encode(bytes_data)
    return b64_bytes.decode("utf-8")

# simple consuming logic from Kafka topic, without considering security for simplicity. In production, you should consider using SSL/TLS and authentication mechanisms.
AAS_URL = f"http://localhost:8081/submodels/{to_base64('urn:aas:submodel:characteristic1:test')}/submodel-elements/temperature"

consumer = KafkaConsumer(
    'sensor-data',
    bootstrap_servers=['localhost:9093'], #h ip tou kafka boker sthn ousia
    group_id='my-group'
)



## consume messages from Kafka and forward to AAS
for msg in consumer:
    # print (msg) #short break before retrying
    print(f"Consumed from topic '{msg.topic}' the message: {msg.value}")
    tst = json.loads(msg.value.decode('utf-8'))
    print(tst["properties"]["temperature"])
    print("Forwarding to AAS...")
    payload = {
            "modelType": "Property",
            "value": str(tst["properties"]["temperature"]),
            "valueType": "xs:double",
            "idShort": "temperature"
        }
    
    response = requests.put(AAS_URL, headers={"Content-Type": "application/json"}, data=json.dumps(payload))
    print(f"AAS response: {response.status_code} - {response.text}")
    time.sleep(1)