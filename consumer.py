import io

from avro.io import BinaryDecoder, BinaryEncoder
from avro.io import DatumReader, DatumWriter
from avro.io import validate
from avro.schema import parse

from kafka import KafkaConsumer
from pandaproxy_registry import SchemaRegistry

broker = "localhost:9092"
registry = "http://localhost:8081"
topic = "clickstream"
id = 1

# Retrieve the schema from the registry
print(f'Retrieving schema from registry: {registry}')
registry = SchemaRegistry(registry)
schema = parse(registry.get_schema(id))
print(schema)

def from_avro(value):
    bytes_reader = io.BytesIO(value)
    decoder = BinaryDecoder(bytes_reader)
    reader = DatumReader(schema)
    return reader.read(decoder)

consumer = KafkaConsumer(
    bootstrap_servers = broker,
    group_id = 'weather-watchers',
    auto_offset_reset = 'earliest',
    value_deserializer = from_avro
)
consumer.subscribe(topic)

# Consume the messages and validate against the Avro schema
print(f'Consuming messages from topic: {topic}...')
while True:
    batch = consumer.poll(timeout_ms=1000)
    if not batch:
        break
    for tp, messages in batch.items():
        for msg in messages:
            print('[valid]' if validate(schema, msg.value) else '[invalid]', msg.value)
