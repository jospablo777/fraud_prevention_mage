from pydantic import BaseModel, Field, conint, ConfigDict
from typing import Optional

# API payloads
class StartRequest(BaseModel):
    interval_secs: Optional[int] = Field(None, ge=1)
    fraud_rate:   Optional[float] = Field(None, ge=0.0, le=1.0)
    batch_min:    Optional[int] = Field(None, ge=1)
    batch_max:    Optional[int] = Field(None, ge=1)

class StatusResponse(BaseModel):
    running: bool
    last_sent_at_epoch: float | None
    last_batch_size: int | None
    interval_secs: int
    fraud_rate: float
    batch_min: int
    batch_max: int
    topic: str
    bootstrap: str

# Kafka message schema (flat, explicit fields)
class TransactionEvent(BaseModel):
    # sampler metadata
    transaction_id: str
    event_time: str
    event_time_ms: int

    # features
    V1: float;  V2: float;  V3: float;  V4: float;  V5: float;  V6: float;  V7: float
    V8: float;  V9: float;  V10: float; V11: float; V12: float; V13: float; V14: float
    V15: float; V16: float; V17: float; V18: float; V19: float; V20: float; V21: float
    V22: float; V23: float; V24: float; V25: float; V26: float; V27: float; V28: float
    Amount: float

    # label
    Class: conint(ge=0, le=1)

    # producer-side metadata (serialized with aliases)
    source_: str | None = Field(default=None, alias="_source")
    schema_version_: int | None = Field(default=None, alias="_schema_version")
    produce_time_ms_: int | None = Field(default=None, alias="_produce_time_ms")

    model_config = ConfigDict(populate_by_name=True)
