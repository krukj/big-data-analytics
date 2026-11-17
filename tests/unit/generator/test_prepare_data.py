"""
Unit tests for generator/prepare_data.py:
batch/stream split of PaySim and generation of synthetic login events.
"""

import numpy as np
import pandas as pd
import pytest
from generator import prepare_data
from generator.prepare_data import (
    DEVICE_COLUMNS,
    LOGIN_SCENARIOS,
    NEAR_WINDOW_HOURS,
    assign_login_scenarios,
    generate_login_events,
    prepare_paysim,
    sample_device_profiles,
    select_accounts,
)

SPLIT_STEP = 500
HOUR = 3600

LOGIN_EVENT_COLUMNS = [
    "event_id",
    "account_id",
    "device_id",
    "event_type",
    "step",
    "second_offset",
    "event_time",
    "device_type",
    "operating_system",
    "browser",
    "device_info",
    "screen_resolution",
]


class TestPreparePaysim:
    def test_split_is_exhaustive_and_disjoint(self, paysim_df):
        """Every row lands in exactly one of batch (step <= 500) or stream (> 500)."""
        batch, stream = prepare_paysim(paysim_df, SPLIT_STEP)

        assert (batch["step"] <= SPLIT_STEP).all()
        assert (stream["step"] > SPLIT_STEP).all()
        assert len(batch) + len(stream) == len(paysim_df)
        assert set(batch.index).isdisjoint(stream.index)

    def test_split_step_itself_belongs_to_batch(self, paysim_df):
        """The boundary step is historical data, not stream."""
        batch, stream = prepare_paysim(paysim_df, SPLIT_STEP)
        assert SPLIT_STEP in set(batch["step"])
        assert SPLIT_STEP not in set(stream["step"])

    def test_event_time_is_derived_from_step(self, paysim_df):
        """event_time = (step - 1) * 3600 and second_offset = 0 for every row."""
        batch, stream = prepare_paysim(paysim_df, SPLIT_STEP)
        for part in (batch, stream):
            assert (part["event_time"] == (part["step"] - 1) * HOUR).all()
            assert (part["second_offset"] == 0).all()
        assert batch.loc[batch["step"] == 1, "event_time"].iloc[0] == 0

    def test_input_frame_is_not_mutated(self, paysim_df):
        """prepare_paysim works on a copy and leaves the input frame untouched."""
        before = paysim_df.copy()
        prepare_paysim(paysim_df, SPLIT_STEP)
        pd.testing.assert_frame_equal(paysim_df, before)

    @pytest.mark.parametrize(
        "split_step, expected_batch_rows", [(0, 0), (499, 3), (743, 12)]
    )
    def test_split_step_boundaries(self, paysim_df, split_step, expected_batch_rows):
        """Extreme split steps give an empty batch (0) or an empty stream (743)."""
        batch, stream = prepare_paysim(paysim_df, split_step)
        assert len(batch) == expected_batch_rows
        assert len(stream) == len(paysim_df) - expected_batch_rows


class TestSelectAccounts:
    def test_first_step_and_count_per_account(self, paysim_df):
        """Per sender: first streaming step and number of transactions."""
        _, stream = prepare_paysim(paysim_df, SPLIT_STEP)
        info = select_accounts(stream).set_index("account_id")

        assert list(info.columns) == ["first_transaction_step", "transaction_count"]
        assert info.loc["C_B", "first_transaction_step"] == 503
        assert info.loc["C_B", "transaction_count"] == 2
        assert info.loc["C_D", "first_transaction_step"] == 520
        assert info.loc["C_D", "transaction_count"] == 2
        assert len(info) == stream["nameOrig"].nunique()
        assert info.index.is_unique

    def test_subsample_returns_requested_number_of_accounts(self, paysim_df):
        """n_accounts limits the selection to that many existing senders."""
        _, stream = prepare_paysim(paysim_df, SPLIT_STEP)
        info = select_accounts(stream, n_accounts=3)
        assert len(info) == 3
        assert set(info["account_id"]) <= set(stream["nameOrig"])


class TestSampleDeviceProfiles:
    def test_each_account_gets_one_complete_known_profile(self, identity_df, paysim_df):
        """Each account gets one device profile taken from complete IEEE-CIS rows."""
        _, stream = prepare_paysim(paysim_df, SPLIT_STEP)
        info = select_accounts(stream)
        out = sample_device_profiles(identity_df, info)

        complete = identity_df.dropna(subset=DEVICE_COLUMNS)[DEVICE_COLUMNS]
        assert len(out) == len(info)
        assert out[DEVICE_COLUMNS].notna().all().all()
        assert set(map(tuple, out[DEVICE_COLUMNS].values)) <= set(
            map(tuple, complete.values)
        )

    def test_device_ids_are_unique_and_sequential(self, identity_df, paysim_df):
        """device_id runs D000001, D000002, ... without gaps or repeats."""
        _, stream = prepare_paysim(paysim_df, SPLIT_STEP)
        out = sample_device_profiles(identity_df, select_accounts(stream))
        assert list(out["device_id"]) == [f"D{i:06d}" for i in range(1, len(out) + 1)]

    def test_device_type_shares_follow_reference_distribution(self, identity_df):
        """Sampled desktop/mobile shares match the reference distribution."""
        # non-null DeviceType in the fixture: desktop 3/5, mobile 2/5
        info = pd.DataFrame(
            {
                "account_id": [f"C{i}" for i in range(5000)],
                "first_transaction_step": 510,
                "transaction_count": 1,
            }
        )
        share = sample_device_profiles(identity_df, info)["DeviceType"].value_counts(
            normalize=True
        )
        assert share["desktop"] == pytest.approx(0.6, abs=0.03)
        assert share["mobile"] == pytest.approx(0.4, abs=0.03)

    def test_raises_when_no_complete_profile_exists(self):
        """Identity data without a single complete profile raises ValueError."""
        identity = pd.DataFrame(
            [["desktop", None, "chrome", None, None]], columns=DEVICE_COLUMNS
        )
        info = pd.DataFrame(
            {
                "account_id": ["C1"],
                "first_transaction_step": [510],
                "transaction_count": [1],
            }
        )
        with pytest.raises(ValueError):
            sample_device_profiles(identity, info)