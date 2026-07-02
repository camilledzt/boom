import io

import fastavro
from confluent_kafka import Consumer

c = Consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": "debug-inspect",
        "auto.offset.reset": "earliest",
    }
)
c.subscribe(["LSST_alerts_results"])

while True:
    msg = c.poll(5.0)
    if msg is None:
        break
    if msg.error():
        break
    reader = fastavro.reader(io.BytesIO(msg.value()))
    for record in reader:
        print(f"candid={record['candid']}  objectId={record['objectId']}")

c.close()
