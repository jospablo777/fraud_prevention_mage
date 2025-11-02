import json
from aiokafka import AIOKafkaProducer

def make_producer(bootstrap: str) -> AIOKafkaProducer:
    # value: Pydantic model or dict → JSON
    return AIOKafkaProducer(
        bootstrap_servers=bootstrap,
        linger_ms=5,
        acks="all",
        enable_idempotence=True,
        value_serializer=lambda v: (
            v.model_dump_json(by_alias=True).encode("utf-8")
            if hasattr(v, "model_dump_json") else json.dumps(v).encode("utf-8")
        ),
        key_serializer=lambda k: k.encode("utf-8"),
    )
