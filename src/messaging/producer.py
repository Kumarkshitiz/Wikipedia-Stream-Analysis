from kafka import KafkaProducer
import json

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: v.encode("utf-8"),
    linger_ms=50,
    batch_size=32768,
    acks="all"
)

def send(topic, key, value):
    producer.send(topic, key=key, value=value)

def flush():
    producer.flush()