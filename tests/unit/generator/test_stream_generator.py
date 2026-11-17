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


def test_check_data_path_accepts_only_parquet(tmp_path):
    """Only .parquet paths are accepted as stream inputs."""
    assert (
        StreamProducer._check_data_path(str(tmp_path / "x.parquet")).suffix
        == ".parquet"
    )
    with pytest.raises(ValueError):
        StreamProducer._check_data_path(str(tmp_path / "x.csv"))


def test_take_next_stream_filters_current_step(two_steps):
    """Only rows of the current simulation step are selected for emission."""
    df, _ = two_steps
    producer = make_producer(df, first_step=502)
    out = producer._take_next_stream(df)
    assert len(out) == 2
    assert (out["step"] == 502).all()


def test_data_corruption_conserves_rows_and_stores_delayed_ones():
    """Emitted + delayed = input + duplicates; the input frame is not mutated."""
    random.seed(0)
    n = 20_000
    df = transactions(n)
    before = df.copy()
    producer = make_producer(df)
    producer.delayed_rows[producer.current_step] = []

    out = producer._data_corruption(df)
    delayed = producer.delayed_rows[producer.current_step]
    duplicates = len(out) - (n - len(delayed))

    pd.testing.assert_frame_equal(df, before)
    assert len(delayed) > 0
    assert duplicates >= 0
    assert len(out) == n - len(delayed) + duplicates
    assert not {row["nameOrig"] for row in delayed} & set(
        out["nameOrig"].iloc[: n - len(delayed)]
    )


def test_fault_rates_match_documented_values():
    """Fault rates match the report: 2 % blanked, 0.5 % duplicated, 1 % delayed."""
    # missing values 2 %, duplicates 0.5 %, out-of-order events 1 %
    random.seed(1)
    n = 20_000
    df = transactions(n)
    producer = make_producer(df)
    producer.delayed_rows[producer.current_step] = []

    out = producer._data_corruption(df)
    delayed = len(producer.delayed_rows[producer.current_step])
    duplicates = len(out) - (n - delayed)
    blanked = int(out[COLS].isna().any(axis=1).sum())

    assert blanked / n == pytest.approx(0.02, abs=0.005)
    assert duplicates / n == pytest.approx(0.005, abs=0.002)
    assert delayed / n == pytest.approx(0.01, abs=0.003)
    assert out[COLS].isna().sum(axis=1).max() <= 1


def test_produce_replays_every_row_once_in_step_order(monkeypatch, two_steps):
    """Without faults every row is sent once, in step order, until an empty step."""
    monkeypatch.setattr(stream_generator.random, "random", ScriptedRandom([]))
    df, logins = two_steps
    producer = make_producer(df, logins)
    producer.produce()

    sent = produced(producer, TRANSACTION_TOPIC)
    sent_logins = produced(producer, LOGIN_TOPIC)

    assert len(sent) == len(df)
    assert len(sent_logins) == len(logins)
    assert [m["step"] for m in sent] == [501, 501, 501, 502, 502]
    assert sorted(m["nameOrig"] for m in sent) == sorted(df["nameOrig"])
    assert set(sent[0]) == set(df.columns)
    assert producer.current_step == 503


def test_delayed_row_is_re_emitted_in_the_following_step(monkeypatch, two_steps):
    """A delayed row is sent in the next step after its own rows; nothing is lost."""
    # first row of step 501: not blanked, not duplicated, delayed
    monkeypatch.setattr(
        stream_generator.random, "random", ScriptedRandom([0.5, 0.5, 0.001])
    )
    df, logins = two_steps
    producer = make_producer(df, logins)
    producer.produce()

    sent = produced(producer, TRANSACTION_TOPIC)

    assert len(sent) == len(df)
    assert [m["step"] for m in sent] == [501, 501, 502, 502, 501]
    assert sent[-1]["nameOrig"] == "C0"
