import json
import os
import threading
import time
import uuid
from typing import Any, Dict, Optional

from fastapi import FastAPI, HTTPException
from kafka import KafkaConsumer, KafkaProducer
from pydantic import BaseModel

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
FEATURE_COLUMNS = [f"V{i}" for i in range(1, 29)] + ["Amount"]
REVIEW_THRESHOLD = 0.7

app = FastAPI(title="CreditGuard Realtime Scoring API", version="1.0.0")
pending: Dict[str, Dict[str, Any]] = {}
pending_events: Dict[str, threading.Event] = {}
pending_lock = threading.Lock()
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)


class TransactionRequest(BaseModel):
    user_id: str = "api-client"
    source: str = "scoring_api"
    transaction_id: Optional[str] = None
    event_timestamp: Optional[float] = None
    model_config = {"extra": "allow"}


def prediction_listener() -> None:
    while True:
        try:
            consumer = KafkaConsumer(
                "all_predictions",
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=f"scoring-api-{uuid.uuid4()}",
                auto_offset_reset="latest",
                value_deserializer=lambda value: json.loads(value.decode("utf-8")),
            )
            for message in consumer:
                result = message.value
                transaction_id = result.get("transaction_id")
                if not transaction_id:
                    continue
                with pending_lock:
                    if transaction_id in pending_events:
                        pending[transaction_id] = result
                        pending_events[transaction_id].set()
        except Exception as error:
            print(f"Prediction listener reconnecting: {error}")
            time.sleep(2)


threading.Thread(target=prediction_listener, daemon=True).start()


@app.get("/health")
def health() -> dict:
    return {"status": "ok", "review_threshold": REVIEW_THRESHOLD}


@app.post("/score")
def score(transaction: TransactionRequest) -> dict:
    payload = transaction.model_dump()
    missing = [column for column in FEATURE_COLUMNS if column not in payload]
    if missing:
        raise HTTPException(status_code=422, detail=f"Missing model features: {missing}")

    transaction_id = payload.get("transaction_id") or str(uuid.uuid4())
    payload["transaction_id"] = transaction_id
    payload["event_timestamp"] = payload.get("event_timestamp") or time.time()

    event = threading.Event()
    with pending_lock:
        pending_events[transaction_id] = event

    try:
        producer.send("transactions", payload).get(timeout=1.0)
        if not event.wait(timeout=1.8):
            raise HTTPException(status_code=504, detail="Scoring did not complete within 2 seconds")
        with pending_lock:
            return pending.pop(transaction_id)
    finally:
        with pending_lock:
            pending_events.pop(transaction_id, None)


@app.get("/alerts")
def alerts() -> dict:
    return {"topic": "fraud_alerts", "review_threshold": REVIEW_THRESHOLD}
