from kafka import KafkaProducer
import json

# ------------------------------------------------
# Kafka Producer Configuration
# ------------------------------------------------

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda v: v.encode("utf-8") if isinstance(v, str) else b"unknown",
    linger_ms=50,
    batch_size=32768,
    acks="all"
)

# ------------------------------------------------
# Send Message
# ------------------------------------------------

def send(topic, key, value):
    # handle None or invalid keys safely
    if key is None or not isinstance(key, str):
        key = "unknown"

    producer.send(topic, key=key, value=value)


# ------------------------------------------------
# Flush Producer
# ------------------------------------------------

def flush():
    producer.flush()