"""
Unit tests for batch_layer/clean_stream_data.py :: StreamConsumer.data_cleaning:
de-duplication and imputation of missing financial fields in stream messages.
"""

import math

import pandas as pd
import pytest
from batch_layer.clean_stream_data import StreamConsumer

OUTFLOW_TYPES = ["CASH_OUT", "PAYMENT", "TRANSFER"]
FINANCIAL_COLUMNS = ["amount", "oldbalanceOrg", "newbalanceOrig"]
NON_FINANCIAL_COLUMNS = [
    "type",
    "nameOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
]


def message(
    typ="CASH_OUT",
    amount=600.0,
    old=1000.0,
    new=400.0,
    step=505,
    orig="C1",
    dest="C9",
    **overrides,
):
    """One Kafka message as emitted by stream_generator (JSON dict, nulls as None)."""
    payload = {
        "step": step,
        "type": typ,
        "amount": amount,
        "nameOrig": orig,
        "oldbalanceOrg": old,
        "newbalanceOrig": new,
        "nameDest": dest,
        "oldbalanceDest": 0.0,
        "newbalanceDest": 0.0,
        "isFraud": 0,
        "isFlaggedFraud": 0,
        "second_offset": 0,
        "event_time": (step - 1) * 3600,
    }
    payload.update(overrides)
    return payload


def is_missing(value):
    return value is None or (isinstance(value, float) and math.isnan(value))


REFERENCE_ROW = message(orig="C_REF")


@pytest.fixture
def cleaner():
    return StreamConsumer()


def clean(cleaner, rows):
    return cleaner.data_cleaning(rows).reset_index(drop=True)


def clean_one(cleaner, faulty_row):
    """
    Clean ``faulty_row`` inside a batch that also holds a complete row and
    return the cleaned faulty row. A realistic micro-batch mixes complete and
    faulty messages; a batch in which a column is null in *every* row is a
    separate case (see ``test_batch_with_fully_null_financial_column``).
    """
    out = clean(cleaner, [faulty_row, REFERENCE_ROW])
    return out.set_index("nameOrig").loc[faulty_row["nameOrig"]]


def test_exact_duplicates_are_removed(cleaner):
    """Identical messages (the generator's duplicate fault) collapse to one row."""
    first = message(orig="C1")
    second = message(orig="C2", amount=5.0, old=5.0, new=0.0)
    out = clean(cleaner, [first, first, second, first])

    assert len(out) == 2
    assert out.duplicated().sum() == 0
    assert set(out["nameOrig"]) == {"C1", "C2"}


@pytest.mark.parametrize(
    "typ, old, new, expected_amount",
    [
        ("CASH_OUT", 1000.0, 400.0, 600.0),
        ("CASH_IN", 400.0, 1000.0, 600.0),
        ("PAYMENT", 50.0, 50.0, 0.0),
    ],
)
def test_missing_amount_is_absolute_balance_difference(
    cleaner, typ, old, new, expected_amount
):
    """A blanked amount is recovered as |oldbalanceOrg - newbalanceOrig|."""
    row = clean_one(cleaner, message(typ=typ, amount=None, old=old, new=new))
    assert row["amount"] == pytest.approx(expected_amount)


@pytest.mark.parametrize("typ", OUTFLOW_TYPES)
def test_missing_old_balance_for_outflow_is_new_plus_amount(cleaner, typ):
    """For CASH_OUT/PAYMENT/TRANSFER a blanked oldbalanceOrg = new + amount."""
    row = clean_one(cleaner, message(typ=typ, amount=600.0, old=None, new=400.0))
    assert row["oldbalanceOrg"] == pytest.approx(1000.0)


def test_missing_old_balance_for_cash_in_is_new_minus_amount(cleaner):
    """For CASH_IN a blanked oldbalanceOrg = new - amount (money flows in)."""
    row = clean_one(cleaner, message(typ="CASH_IN", amount=600.0, old=None, new=1000.0))
    assert row["oldbalanceOrg"] == pytest.approx(400.0)


@pytest.mark.parametrize("typ", OUTFLOW_TYPES)
def test_missing_new_balance_for_outflow_is_old_minus_amount(cleaner, typ):
    """For CASH_OUT/PAYMENT/TRANSFER a blanked newbalanceOrig = old - amount."""
    row = clean_one(cleaner, message(typ=typ, amount=600.0, old=1000.0, new=None))
    assert row["newbalanceOrig"] == pytest.approx(400.0)


def test_missing_new_balance_for_cash_in_is_old_plus_amount(cleaner):
    """For CASH_IN a blanked newbalanceOrig = old + amount."""
    row = clean_one(cleaner, message(typ="CASH_IN", amount=600.0, old=400.0, new=None))
    assert row["newbalanceOrig"] == pytest.approx(1000.0)


def test_complete_rows_pass_through_unchanged(cleaner):
    """Messages without faults keep every value and the row order."""
    rows = [
        message(orig="C1"),
        message(typ="CASH_IN", amount=10.0, old=0.0, new=10.0, orig="C2"),
    ]
    out = clean(cleaner, rows)
    pd.testing.assert_frame_equal(out, pd.DataFrame(rows), check_dtype=False)


def test_login_events_are_only_deduplicated(cleaner):
    """Login events have no financial columns: only de-duplication applies."""
    login = {
        "event_id": "E0000001",
        "account_id": "C1",
        "device_id": "D000001",
        "event_type": "LOGIN_FAILED",
        "step": 505,
        "second_offset": 10,
        "event_time": 504 * 3600 + 10,
        "device_type": "mobile",
        "operating_system": "iOS 11.1.2",
        "browser": "mobile safari 11.0",
        "device_info": "iOS Device",
        "screen_resolution": "2208x1242",
    }
    out = clean(cleaner, [login, login])

    assert len(out) == 1
    assert list(out.columns) == list(login.keys())
    assert out.iloc[0].to_dict() == login


def test_no_residual_nulls_in_financial_columns_after_single_fault(cleaner):
    """One blanked financial field per row, any type: no null survives cleaning."""
    rows = [
        message(typ=typ, **{column: None})
        for column in FINANCIAL_COLUMNS
        for typ in OUTFLOW_TYPES + ["CASH_IN"]
    ]
    out = clean(cleaner, rows)

    assert len(out) == len(rows)
    assert out[FINANCIAL_COLUMNS].isna().sum().sum() == 0


def test_imputed_rows_satisfy_balance_arithmetic(cleaner):
    """Imputed values keep old ± amount = new consistent for every type."""
    rows = [
        message(typ="CASH_OUT", amount=None, old=1000.0, new=400.0),
        message(typ="TRANSFER", amount=600.0, old=None, new=400.0),
        message(typ="PAYMENT", amount=600.0, old=1000.0, new=None),
        message(typ="CASH_IN", amount=None, old=400.0, new=1000.0),
        message(typ="CASH_IN", amount=600.0, old=None, new=1000.0),
        message(typ="CASH_IN", amount=600.0, old=400.0, new=None),
    ]
    out = clean(cleaner, rows)
    sign = out["type"].map(lambda typ: 1 if typ == "CASH_IN" else -1)
    residual = out["oldbalanceOrg"] + sign * out["amount"] - out["newbalanceOrig"]
    assert residual.abs().max() == pytest.approx(0.0)


@pytest.mark.parametrize("column", FINANCIAL_COLUMNS)
def test_batch_with_fully_null_financial_column_is_cleaned(cleaner, column):
    """Cleaning works even when a financial column is null in every row."""
    out = clean(
        cleaner,
        [message(orig="C1", **{column: None}), message(orig="C2", **{column: None})],
    )
    assert len(out) == 2
    assert out[column].notna().all()
