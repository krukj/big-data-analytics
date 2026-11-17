"""
End-to-end test of the stream path: Kafka -> Airflow DAG -> cleaning -> MinIO.

Publishes a small, marked batch of transactions (one with a blanked amount),
triggers the stream_to_minio DAG and waits for the cleaned parquet to appear
in the clean-transactions-data bucket.

Require the full stack except Spark and Cassandra:
    docker compose up -d kafka kafka-init minio batch-layer \
        postgres airflow-init airflow-scheduler
"""

import io
import json
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
import pytest
from confluent_kafka import Producer

from config import KAFKA_BROKER, TRANSACTION_TOPIC

pytestmark = pytest.mark.integration

BUCKET = "clean-transactions-data"
DAG_ID = "stream_to_minio"
FINANCIAL_COLUMNS = ["amount", "oldbalanceOrg", "newbalanceOrig"]
WAIT_SECONDS = 300
POLL_SECONDS = 10


def transaction(marker: str, index: int, amount: float | None) -> dict:
    return {
        "step": 999,
        "type": "CASH_OUT",
        "amount": amount,
        "nameOrig": f"{marker}_{index}",
        "oldbalanceOrg": 1000.0,
        "newbalanceOrig": 400.0,
        "nameDest": "C_SMOKE_DEST",
        "oldbalanceDest": 0.0,
        "newbalanceDest": 0.0,
        "isFraud": 0,
        "isFlaggedFraud": 0,
        "second_offset": 0,
        "event_time": 998 * 3600,
    }


def newest_objects(s3, since: datetime) -> list[dict]:
    listing = s3.list_objects_v2(Bucket=BUCKET).get("Contents", [])
    return sorted(
        (o for o in listing if o["LastModified"] >= since),
        key=lambda o: o["LastModified"],
        reverse=True,
    )


def test_marked_batch_is_cleaned_and_stored(kafka_admin, s3, airflow):
    """Ten marked messages reach MinIO cleaned: all present, blanked amount imputed."""
    marker = f"C_SMOKE_{uuid.uuid4().hex[:8]}"
    started = datetime.now(timezone.utc)

    producer = Producer({"bootstrap.servers": KAFKA_BROKER, "acks": "all"})
    for index in range(10):
        amount = None if index == 0 else 600.0
        producer.produce(
            TRANSACTION_TOPIC, value=json.dumps(transaction(marker, index, amount))
        )
    assert producer.flush(10) == 0, "not all messages were delivered to Kafka"

    airflow("dags", "unpause", DAG_ID)
    airflow("dags", "trigger", DAG_ID)

    rows = None
    deadline = time.monotonic() + WAIT_SECONDS
    while time.monotonic() < deadline:
        for obj in newest_objects(s3, started):
            body = s3.get_object(Bucket=BUCKET, Key=obj["Key"])["Body"].read()
            frame = pd.read_parquet(io.BytesIO(body))
            marked = frame[frame["nameOrig"].str.startswith(marker)]
            if len(marked) == 10:
                rows = marked
                break
        if rows is not None:
            break
        time.sleep(POLL_SECONDS)

    assert rows is not None, (
        f"no cleaned parquet with the {marker} batch within {WAIT_SECONDS}s"
    )
    assert rows[FINANCIAL_COLUMNS].isna().sum().sum() == 0
    imputed = rows.loc[rows["nameOrig"] == f"{marker}_0", "amount"].iloc[0]
    assert imputed == pytest.approx(600.0)
