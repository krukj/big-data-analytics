"""
Unit-test fixtures: pure Python, no Docker, Kafka or MinIO.

``confluent_kafka.Consumer``/``Producer`` are swapped for inert fakes before the
production modules are imported, because ``StreamConsumer`` creates a Consumer at
import time (class attribute).
"""

import confluent_kafka
import numpy as np
import pandas as pd
import pytest

class FakeKafkaClient:
    """Accepts any confluent_kafka configuration and records produced messages."""

    def __init__(self, conf=None, *args, **kwargs):
        self.produced = []

    def subscribe(self, topics=None, **kwargs):
        pass

    def produce(self, topic=None, value=None, key=None, **kwargs):
        self.produced.append((topic, key, value))

    def flush(self, *args, **kwargs):
        return 0

confluent_kafka.Consumer = FakeKafkaClient
confluent_kafka.Producer = FakeKafkaClient

PAYSIM_COLUMNS = ["step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig",
    "nameDest", "oldbalanceDest", "newbalanceDest", "isFraud", "isFlaggedFraud"]
DEVICE_COLUMNS = ["DeviceType", "id_30", "id_31", "DeviceInfo", "id_33"]

def transaction(step, typ, amount, orig, old, new, dest="M100", old_dest=0.0, new_dest=0.0):
    return {
        "step": step,
        "type": typ,
        "amount": float(amount),
        "nameOrig": orig,
        "oldbalanceOrg": float(old),
        "newbalanceOrig": float(new),
        "nameDest": dest,
        "oldbalanceDest": float(old_dest),
        "newbalanceDest": float(new_dest),
        "isFraud": 0,
        "isFlaggedFraud": 0,
    }


@pytest.fixture
def paysim_df() -> pd.DataFrame:
    """Twelve PaySim-like rows on both sides of the batch/stream split (step 500)."""
    rows = [
        transaction(1, "PAYMENT", 100, "C_H1", 1000, 900),
        transaction(250, "CASH_IN", 50, "C_H2", 100, 150),
        transaction(499, "TRANSFER", 300, "C_H3", 300, 0, dest="C900", new_dest=300),
        transaction(500, "CASH_OUT", 300, "C_H4", 300, 0, dest="C901"),
        transaction(501, "PAYMENT", 10, "C_A", 50, 40),
        transaction(503, "CASH_OUT", 20, "C_B", 70, 50, dest="C902"),
        transaction(505, "TRANSFER", 30, "C_B", 50, 20, dest="C903"),
        transaction(510, "CASH_IN", 40, "C_C", 10, 50),
        transaction(520, "DEBIT", 5, "C_D", 15, 10),
        transaction(520, "PAYMENT", 7, "C_D", 10, 3),
        transaction(600, "CASH_OUT", 9, "C_E", 9, 0, dest="C904"),
        transaction(743, "PAYMENT", 1, "C_F", 2, 1),
    ]
    return pd.DataFrame(rows, columns=PAYSIM_COLUMNS)


@pytest.fixture
def identity_df() -> pd.DataFrame:
    """IEEE-CIS-like identity rows: three complete profiles, two incomplete."""
    return pd.DataFrame(
        [
            ["desktop", "Windows 10", "chrome 63.0", "Windows", "1920x1080"],
            ["desktop", "Windows 10", "chrome 63.0", "Windows", "1920x1080"],
            ["mobile", "iOS 11.1.2", "mobile safari 11.0", "iOS Device", "2208x1242"],
            [
                "mobile",
                "Android 7.0",
                "chrome 63.0 for android",
                "SM-G930V",
                "1920x1080",
            ],
            ["desktop", None, "firefox 57.0", "MacOS", None],
            [None, None, None, None, None],
        ],
        columns=DEVICE_COLUMNS,
    )


@pytest.fixture
def accounts_with_devices() -> pd.DataFrame:
    """
    One account per login scenario (SHORT_ACTIVITY, first transaction at step 520)
    plus two edge cases: first step 501 (must be skipped) and 502 (one-hour window).
    """
    device = {
        "DeviceType": "mobile",
        "id_30": "iOS 11.1.2",
        "id_31": "mobile safari 11.0",
        "DeviceInfo": "iOS Device",
        "id_33": "2208x1242",
    }
    scenarios = [
        "NORMAL_LOGIN",
        "SINGLE_FAILURE",
        "MULTIPLE_FAILURES",
        "MULTIPLE_LOGINS",
    ]
    rows = [
        {
            "account_id": f"C_{scenario}",
            "first_transaction_step": 520,
            "transaction_count": 1,
            "device_id": f"D{i:06d}",
            "activity_pattern": "SHORT_ACTIVITY",
            "login_scenario": scenario,
            **device,
        }
        for i, scenario in enumerate(scenarios, start=1)
    ]
    rows.append(
        {
            "account_id": "C_SKIP_501",
            "first_transaction_step": 501,
            "transaction_count": 1,
            "device_id": "D000005",
            "activity_pattern": "SHORT_ACTIVITY",
            "login_scenario": "NORMAL_LOGIN",
            **device,
        }
    )
    rows.append(
        {
            "account_id": "C_EDGE_502",
            "first_transaction_step": 502,
            "transaction_count": 1,
            "device_id": "D000006",
            "activity_pattern": "MIXED_ACTIVITY",
            "login_scenario": "MULTIPLE_LOGINS",
            **device,
        }
    )
    return pd.DataFrame(rows)


@pytest.fixture(autouse=True)
def seed_numpy():
    """Every test starts from the project seed, so results are reproducible."""
    np.random.seed(42)
