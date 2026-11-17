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