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
        """Verify the batch/stream split loses and duplicates no PaySim row.
        
        Steps:
        1. Take the 12-row PaySim fixture with steps on both sides of step 500.
        2. Call prepare_paysim(paysim_df, 500).
        Expected:
        - Batch holds only steps <= 500 and stream only steps > 500.
        - Row counts add up to the input; the two index sets are disjoint.
        """
        batch, stream = prepare_paysim(paysim_df, SPLIT_STEP)

        assert (batch["step"] <= SPLIT_STEP).all()
        assert (stream["step"] > SPLIT_STEP).all()
        assert len(batch) + len(stream) == len(paysim_df)
        assert set(batch.index).isdisjoint(stream.index)

    def test_split_step_itself_belongs_to_batch(self, paysim_df):
        """Verify the boundary step 500 is assigned to the historical batch.
        
        Steps:
        1. Call prepare_paysim(paysim_df, 500); the fixture has a row at step 500.
        2. Collect the distinct step values of both outputs.
        Expected:
        - Step 500 occurs in the batch and not in the stream.
        """
        batch, stream = prepare_paysim(paysim_df, SPLIT_STEP)
        assert SPLIT_STEP in set(batch["step"])
        assert SPLIT_STEP not in set(stream["step"])

    def test_event_time_is_derived_from_step(self, paysim_df):
        """Verify the added time attributes are consistent with the PaySim step.
        
        Steps:
        1. Call prepare_paysim(paysim_df, 500).
        2. Compare event_time and second_offset with step in both outputs.
        Expected:
        - event_time equals (step - 1) * 3600 in every row; step 1 maps to 0.
        - second_offset equals 0 in every row.
        """
        batch, stream = prepare_paysim(paysim_df, SPLIT_STEP)
        for part in (batch, stream):
            assert (part["event_time"] == (part["step"] - 1) * HOUR).all()
            assert (part["second_offset"] == 0).all()
        assert batch.loc[batch["step"] == 1, "event_time"].iloc[0] == 0

    def test_input_frame_is_not_mutated(self, paysim_df):
        """Verify prepare_paysim leaves the caller's DataFrame unchanged.
        
        Steps:
        1. Copy the PaySim fixture.
        2. Call prepare_paysim on the original frame.
        Expected:
        - The original frame equals the copy (same columns and values).
        """
        before = paysim_df.copy()
        prepare_paysim(paysim_df, SPLIT_STEP)
        pd.testing.assert_frame_equal(paysim_df, before)

    @pytest.mark.parametrize(
        "split_step, expected_batch_rows", [(0, 0), (499, 3), (743, 12)]
    )
    def test_split_step_boundaries(self, paysim_df, split_step, expected_batch_rows):
        """Verify the split at extreme and intermediate split steps.
        
        Steps:
        1. Call prepare_paysim with split_step 0, 499 and 743 (one case each).
        2. Count the rows of batch and stream.
        Expected:
        - Batch holds 0, 3 and 12 rows respectively.
        - Stream holds the remaining rows of the 12-row fixture.
        """
        batch, stream = prepare_paysim(paysim_df, split_step)
        assert len(batch) == expected_batch_rows
        assert len(stream) == len(paysim_df) - expected_batch_rows


class TestSelectAccounts:
    def test_first_step_and_count_per_account(self, paysim_df):
        """Verify select_accounts aggregates the stream per sending account.
        
        Steps:
        1. Split the fixture at step 500 and pass the stream to select_accounts.
        2. Read the rows of accounts C_B and C_D.
        Expected:
        - Output has account_id, first_transaction_step and transaction_count.
        - C_B: first step 503, 2 transactions; C_D: first step 520, 2.
        - There is exactly one row per distinct nameOrig.
        """
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
        """Verify n_accounts limits the selection to that many existing senders.
        
        Steps:
        1. Split the fixture at step 500.
        2. Call select_accounts(stream, n_accounts=3).
        Expected:
        - Exactly 3 accounts are returned.
        - Every returned account_id is a nameOrig of the stream.
        """
        _, stream = prepare_paysim(paysim_df, SPLIT_STEP)
        info = select_accounts(stream, n_accounts=3)
        assert len(info) == 3
        assert set(info["account_id"]) <= set(stream["nameOrig"])


class TestSampleDeviceProfiles:
    def test_each_account_gets_one_complete_known_profile(self, identity_df, paysim_df):
        """Verify every account receives one complete IEEE-CIS device profile.
        
        Steps:
        1. Select all stream accounts of the PaySim fixture.
        2. Call sample_device_profiles with the identity fixture, which mixes complete
            and incomplete rows.
        Expected:
        - There is one output row per account and no device column is null.
        - Every sampled profile exists among the complete identity rows.
        """
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
        """Verify device identifiers are unique and sequential.
        
        Steps:
        1. Call sample_device_profiles for all stream accounts of the fixture.
        Expected:
        - device_id runs D000001, D000002, ... in row order without gaps.
        """
        _, stream = prepare_paysim(paysim_df, SPLIT_STEP)
        out = sample_device_profiles(identity_df, select_accounts(stream))
        assert list(out["device_id"]) == [f"D{i:06d}" for i in range(1, len(out) + 1)]

    def test_device_type_shares_follow_reference_distribution(self, identity_df):
        """Verify sampled device types follow the reference distribution.
        
        Steps:
        1. Build 5000 synthetic accounts.
        2. Call sample_device_profiles with the identity fixture (non-null DeviceType:
            desktop 3/5, mobile 2/5).
        3. Compute the share of each sampled DeviceType.
        Expected:
        - Desktop share is 0.60 +/- 0.03 and mobile share is 0.40 +/- 0.03.
        """
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
        """Verify identity data without any complete profile is rejected.
        
        Steps:
        1. Build identity data whose only row has null device columns.
        2. Call sample_device_profiles for one account.
        Expected:
        - ValueError is raised.
        """
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


class TestAssignLoginScenarios:
    def test_values_come_from_documented_domains(self):
        """Verify activity patterns and login scenarios take documented values.
        
        Steps:
        1. Call assign_login_scenarios for 100 accounts.
        Expected:
        - There is one output row per account.
        - activity_pattern is SHORT_ACTIVITY or MIXED_ACTIVITY and login_scenario is a
            key of LOGIN_SCENARIOS.
        - The input frame gets no login_scenario column.
        """
        accounts = pd.DataFrame({"account_id": [f"C{i}" for i in range(100)]})
        out = assign_login_scenarios(accounts)

        assert len(out) == len(accounts)
        assert set(out["activity_pattern"]) <= {"SHORT_ACTIVITY", "MIXED_ACTIVITY"}
        assert set(out["login_scenario"]) <= set(LOGIN_SCENARIOS)
        assert "login_scenario" not in accounts.columns

    def test_shares_match_documented_mix(self):
        """Verify scenario and activity shares match the documented mix.

        Steps:
        1. Call assign_login_scenarios for 20 000 accounts.
        2. Compute the share of each activity pattern and login scenario.
        Expected:
        - SHORT_ACTIVITY share is 0.70 +/- 0.015.
        - NORMAL_LOGIN, SINGLE_FAILURE, MULTIPLE_FAILURES and MULTIPLE_LOGINS shares are
            0.65, 0.20, 0.05 and 0.10, each +/- 0.015.
        """
        # 70/30 activity patterns, 65/20/5/10 login scenarios
        accounts = pd.DataFrame({"account_id": [f"C{i}" for i in range(20000)]})
        out = assign_login_scenarios(accounts)

        activity = out["activity_pattern"].value_counts(normalize=True)
        scenario = out["login_scenario"].value_counts(normalize=True)
        assert activity["SHORT_ACTIVITY"] == pytest.approx(0.70, abs=0.015)
        assert scenario["NORMAL_LOGIN"] == pytest.approx(0.65, abs=0.015)
        assert scenario["SINGLE_FAILURE"] == pytest.approx(0.20, abs=0.015)
        assert scenario["MULTIPLE_FAILURES"] == pytest.approx(0.05, abs=0.015)
        assert scenario["MULTIPLE_LOGINS"] == pytest.approx(0.10, abs=0.015)


class TestGenerateLoginEvents:
    @pytest.fixture
    def events(self, accounts_with_devices):
        return generate_login_events(accounts_with_devices, SPLIT_STEP)

    def test_output_schema(self, events):
        """Verify the schema and identifiers of generated login events.
        
        Steps:
        1. Call generate_login_events(accounts_with_devices, 500).
        Expected:
        - Columns are the 12 documented ones, in the documented order.
        - event_id runs E0000001, E0000002, ... in row order.
        """
        assert list(events.columns) == LOGIN_EVENT_COLUMNS
        assert list(events["event_id"]) == [
            f"E{i:07d}" for i in range(1, len(events) + 1)
        ]

    def test_accounts_starting_at_or_before_split_plus_one_are_skipped(self, events):
        """Verify accounts without a pre-transaction window get no login events.
        
        Steps:
        1. Generate events for the fixture, which holds accounts with first transaction
            step 501 (C_SKIP_501) and 502 (C_EDGE_502).
        Expected:
        - C_SKIP_501 has no events.
        - C_EDGE_502 has events.
        """
        assert "C_SKIP_501" not in set(events["account_id"])
        assert "C_EDGE_502" in set(events["account_id"])

    def test_event_sequence_matches_login_scenario(self, events, accounts_with_devices):
        """Verify each account's event types follow its login scenario.
        
        Steps:
        1. Generate events for the fixture (one account per scenario).
        2. Order the events of each eligible account by event_time.
        Expected:
        - Event types equal LOGIN_SCENARIOS[login_scenario], in order.
        - event_time is strictly increasing within an account.
        """
        eligible = accounts_with_devices[
            accounts_with_devices["first_transaction_step"] > SPLIT_STEP + 1
        ]
        for _, account in eligible.iterrows():
            sequence = events[
                events["account_id"] == account["account_id"]
            ].sort_values("event_time")
            assert (
                list(sequence["event_type"])
                == LOGIN_SCENARIOS[account["login_scenario"]]
            )
            assert sequence["event_time"].is_monotonic_increasing
            assert sequence["event_time"].is_unique

    def test_events_lie_between_split_and_first_transaction(
        self, events, accounts_with_devices
    ):
        """Verify login events fall between the split and the first transaction.
        
        Steps:
        1. Generate events for the fixture.
        2. Compute each account's transaction start as (first_transaction_step - 1) *
            3600.
        Expected:
        - event_time >= 500 * 3600 for every event.
        - event_time is lower than the transaction start of its account.
        """
        first_step = accounts_with_devices.set_index("account_id")[
            "first_transaction_step"
        ]
        transaction_start = (
            events["account_id"].map(first_step).astype(int) - 1
        ) * HOUR

        assert (events["event_time"] < transaction_start).all()
        assert (events["event_time"] >= SPLIT_STEP * HOUR).all()

    def test_short_activity_events_fall_in_near_window(
        self, events, accounts_with_devices
    ):
        """Verify SHORT_ACTIVITY events stay within six hours of the transaction.
        
        Steps:
        1. Generate events for the fixture.
        2. Keep the events of SHORT_ACTIVITY accounts.
        Expected:
        - event_time >= transaction start - NEAR_WINDOW_HOURS * 3600 (6 h).
        """
        first_step = accounts_with_devices.set_index("account_id")[
            "first_transaction_step"
        ]
        short = accounts_with_devices.loc[
            accounts_with_devices["activity_pattern"] == "SHORT_ACTIVITY", "account_id"
        ]
        subset = events[events["account_id"].isin(short)]
        transaction_start = (
            subset["account_id"].map(first_step).astype(int) - 1
        ) * HOUR
        assert (
            subset["event_time"] >= transaction_start - NEAR_WINDOW_HOURS * HOUR
        ).all()

    def test_gaps_between_events_respect_scenario_limits(self, events):
        """Verify gaps between consecutive events respect the scenario limits.
        
        Steps:
        1. Generate events for the fixture.
        2. Compute the gaps between consecutive events of each scenario account.
        Expected:
        - NORMAL_LOGIN has a single event, hence no gap.
        - Gaps are 1 to 120 s for SINGLE_FAILURE and 1 to 60 s for MULTIPLE_FAILURES.
        - Gaps are 1 s to 3 h for MULTIPLE_LOGINS.
        """

        def gaps(account_id):
            times = events.loc[
                events["account_id"] == account_id, "event_time"
            ].sort_values()
            return times.diff().dropna().tolist()

        assert gaps("C_NORMAL_LOGIN") == []
        assert all(1 <= gap <= 120 for gap in gaps("C_SINGLE_FAILURE"))
        assert all(1 <= gap <= 60 for gap in gaps("C_MULTIPLE_FAILURES"))
        assert all(1 <= gap <= 3 * HOUR for gap in gaps("C_MULTIPLE_LOGINS"))

    def test_step_and_second_offset_are_consistent_with_event_time(self, events):
        """Verify step and second_offset are consistent with event_time.
        
        Steps:
        1. Generate events for the fixture.
        2. Recompute step and second_offset from event_time.
        Expected:
        - step equals event_time // 3600 + 1 for every event.
        - second_offset equals event_time % 3600 for every event.
        """
        assert (events["step"] == events["event_time"] // HOUR + 1).all()
        assert (events["second_offset"] == events["event_time"] % HOUR).all()

    def test_device_profile_is_copied_and_renamed(self, events, accounts_with_devices):
        """Verify events carry the account's device profile under the new names.
        
        Steps:
        1. Generate events for the fixture.
        2. Compare each event with the row of its account.
        Expected:
        - device_id equals the account's device_id.
        - device_type, operating_system, browser, device_info and screen_resolution
            equal DeviceType, id_30, id_31, DeviceInfo and id_33.
        """
        accounts = accounts_with_devices.set_index("account_id")
        renamed = {
            "device_type": "DeviceType",
            "operating_system": "id_30",
            "browser": "id_31",
            "device_info": "DeviceInfo",
            "screen_resolution": "id_33",
        }
        for _, event in events.iterrows():
            account = accounts.loc[event["account_id"]]
            assert event["device_id"] == account["device_id"]
            for new_name, old_name in renamed.items():
                assert event[new_name] == account[old_name]

    def test_output_is_sorted_by_event_time(self, events):
        """Verify login events are returned in chronological order.
        
        Steps:
        1. Generate events for the fixture.
        Expected:
        - event_time is non-decreasing over the output rows.
        """
        assert events["event_time"].is_monotonic_increasing

    def test_account_with_one_hour_window_uses_that_hour(self, events):
        """Verify an account with a one-hour window gets its events in that hour.
        
        Steps:
        1. Generate events for the fixture.
        2. Select the events of C_EDGE_502 (MULTIPLE_LOGINS, first step 502).
        Expected:
        - Two events exist.
        - Both lie in [500 * 3600, 501 * 3600).
        """
        times = events.loc[events["account_id"] == "C_EDGE_502", "event_time"]
        assert len(times) == 2
        assert ((times >= SPLIT_STEP * HOUR) & (times < (SPLIT_STEP + 1) * HOUR)).all()


def test_save_data_writes_three_parquet_files(
    tmp_path, monkeypatch, paysim_df, accounts_with_devices
):
    """Verify save_data writes the three parquet outputs with all rows.
    
    Steps:
    1. Redirect PROCESSED_DIR and RAW_STREAM_DIR to a temporary directory.
    2. Build batch, stream and login events and call save_data.
    3. Read the three parquet files back.
    Expected:
    - paysim_batch.parquet, paysim_stream.parquet and login_events.parquet exist.
    - Each file has as many rows as the frame it was written from.
    """
    pytest.importorskip("pyarrow")
    monkeypatch.setattr(prepare_data, "PROCESSED_DIR", tmp_path / "processed")
    monkeypatch.setattr(prepare_data, "RAW_STREAM_DIR", tmp_path / "raw_stream")

    batch, stream = prepare_paysim(paysim_df, SPLIT_STEP)
    events = generate_login_events(accounts_with_devices, SPLIT_STEP)
    prepare_data.save_data(batch, stream, events)

    assert len(pd.read_parquet(tmp_path / "processed" / "paysim_batch.parquet")) == len(
        batch
    )
    assert len(
        pd.read_parquet(tmp_path / "raw_stream" / "paysim_stream.parquet")
    ) == len(stream)
    assert len(
        pd.read_parquet(tmp_path / "raw_stream" / "login_events.parquet")
    ) == len(events)


def _run_generation(paysim_df, identity_df):
    np.random.seed(prepare_data.RANDOM_SEED)
    _, stream = prepare_paysim(paysim_df, SPLIT_STEP)
    accounts = sample_device_profiles(identity_df, select_accounts(stream))
    return generate_login_events(assign_login_scenarios(accounts), SPLIT_STEP)


def test_generation_is_deterministic_under_project_seed(paysim_df, identity_df):
    """Verify login-event generation is reproducible under the project seed.
    
    Steps:
    1. Seed NumPy with RANDOM_SEED and run split, account selection, device sampling,
        scenario assignment and event generation.
    2. Repeat the run with the same seed.
    Expected:
    - The first result is not empty.
    - Both results are identical frames.
    """
    first = _run_generation(paysim_df, identity_df)
    second = _run_generation(paysim_df, identity_df)
    assert not first.empty
    pd.testing.assert_frame_equal(first, second)
