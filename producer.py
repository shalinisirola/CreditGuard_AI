import csv
import json
import os
import random
import time
import uuid
from pathlib import Path

from kafka import KafkaProducer

# 1. Connect to Kafka
# We try to connect until it succeeds (in case Kafka is slow to start)
producer = None
while producer is None:
    try:
        producer = KafkaProducer(
            bootstrap_servers='localhost:9092',
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        print("Connected to Kafka!")
    except Exception as e:
        print("Waiting for Kafka...", e)
        time.sleep(5)

# Keep the online contract identical to train_model_kaggle.py.
feature_columns = [f"V{i}" for i in range(1, 29)] + ["Amount"]
dataset_path = Path(os.getenv("KAGGLE_DATASET_PATH", "creditcard.csv"))
if not dataset_path.exists():
    raise SystemExit(f"Dataset not found: {dataset_path}. Upload creditcard.csv before starting the producer.")

with dataset_path.open(newline="") as dataset_file:
    rows = list(csv.DictReader(dataset_file))
if not rows:
    raise SystemExit("creditcard.csv does not contain any transactions.")

rate_per_second = float(os.getenv("TRANSACTIONS_PER_SECOND", "100"))
delay = 1.0 / rate_per_second
print(f"Loaded {len(rows):,} Kaggle rows; streaming at {rate_per_second:g} transactions/second")

while True:
    row = random.choice(rows)
    transaction = {
        "transaction_id": str(uuid.uuid4()),
        "user_id": "kaggle-replay",
        "event_timestamp": time.time(),
        "source": "kaggle_replay",
    }
    transaction.update({column: float(row[column]) for column in feature_columns})
    producer.send('transactions', transaction)
    time.sleep(delay)
