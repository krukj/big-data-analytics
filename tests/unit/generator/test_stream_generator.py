"""
Unit tests for generator/stream_generator.py :: StreamProducer:
step-wise replay of the PaySim stream with controlled fault injection.
"""

import json
import random

import pandas as pd
import pytest
from confluent_kafka import Producer
from generator import stream_generator
from generator.stream_generator import COLS, StreamProducer

TRANSACTION_TOPIC = "raw-transactions"
LOGIN_TOPIC = "raw-logins"


def make_producer(df, login_df=None, first_step=501):
    """Build a StreamProducer without reading parquet files or contacting Kafka."""
    producer = StreamProducer.__new__(StreamProducer)
    producer.producer = Producer({})
    producer.transaction_topic = TRANSACTION_TOPIC
    producer.login_topic = LOGIN_TOPIC
    producer.latency = 0
    producer.current_step = first_step
    producer.df = df
    producer.login_df = (
        login_df if login_df is not None else pd.DataFrame(columns=["step"])
    )
    producer.delayed_rows = {}
    return producer


def transactions(n, step=501, first_id=0):
    types = ["CASH_OUT", "PAYMENT", "CASH_IN", "TRANSFER", "DEBIT"]
    return pd.DataFrame(
        {
            "step": step,
            "type": [types[i % len(types)] for i in range(n)],
            "amount": [float(i + 1) for i in range(n)],
            "nameOrig": [f"C{first_id + i}" for i in range(n)],
            "oldbalanceOrg": 100.0,
            "newbalanceOrig": 50.0,
            "nameDest": "M1",
            "oldbalanceDest": 0.0,
            "newbalanceDest": 0.0,
            "isFraud": 0,
            "isFlaggedFraud": 0,
            "second_offset": 0,
            "event_time": (step - 1) * 3600,
        }
    )


class ScriptedRandom:
    """Replacement for random.random(): scripted draws first, then 1.0 (no fault)."""

    def __init__(self, draws):
        self._draws = iter(draws)

    def __call__(self):
        return next(self._draws, 1.0)


def produced(producer, topic):
    return [
        json.loads(value) for t, _, value in producer.producer.produced if t == topic
    ]


@pytest.fixture
def two_steps():
    df = pd.concat(
        [transactions(3, step=501), transactions(2, step=502, first_id=3)],
        ignore_index=True,
    )
    logins = pd.DataFrame(
        [
            {
                "event_id": "E1",
                "account_id": "C0",
                "event_type": "LOGIN_SUCCESS",
                "step": 501,
                "event_time": 500 * 3600,
            }
        ]
    )
    return df, logins