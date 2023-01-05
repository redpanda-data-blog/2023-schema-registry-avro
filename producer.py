import io

from avro.io import BinaryDecoder, BinaryEncoder
from avro.io import DatumReader, DatumWriter
from avro.io import validate
from avro.schema import parse
import random
import datetime

from kafka import KafkaProducer
from pandaproxy_registry import SchemaRegistry

registry = "http://localhost:8081"

# Publish the schema
with open('schemas/click_event.avsc') as f:
    schema = parse(f.read())

print(f'Publishing schema to registry: {registry}')
registry = SchemaRegistry(registry)
id = registry.publish(schema.to_json())
print(f'Schema id: {id}')

broker = "localhost:9092"
topic = "clickstream"

def as_avro(value):    
    writer = DatumWriter(schema)
    bytes_writer = io.BytesIO()
    encoder = BinaryEncoder(bytes_writer)
    writer.write(value, encoder)
    return bytes_writer.getvalue()

producer = KafkaProducer(
    bootstrap_servers = broker,
    compression_type = 'gzip',
    value_serializer = as_avro
)

msg_count = 10
event_types = ["PAGE_VIEW", "BUTTON_CLICK", "FORM_FILL"]

for i in range(msg_count):
    user_id = random.randint(1, 100000)
    event_type = random.choice(event_types)
    ts = datetime.datetime.now()

    msg = {"user_id": user_id,
                      "event_type": event_type,
                      "ts": ts.isoformat()}
    producer.send(topic, value=msg)
    print("Produced %d messages.", i)

