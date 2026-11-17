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


class TestAssignLoginScenarios:
    def test_values_come_from_documented_domains(self):
        """activity_pattern and login_scenario only take the documented values."""
        accounts = pd.DataFrame({"account_id": [f"C{i}" for i in range(100)]})
        out = assign_login_scenarios(accounts)

        assert len(out) == len(accounts)
        assert set(out["activity_pattern"]) <= {"SHORT_ACTIVITY", "MIXED_ACTIVITY"}
        assert set(out["login_scenario"]) <= set(LOGIN_SCENARIOS)
        assert "login_scenario" not in accounts.columns

    def test_shares_match_documented_mix(self):
        """Scenario mix 65/20/5/10 and activity mix 70/30 hold at n = 20 000."""
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
        """Login events have the 12 documented columns and sequential event_ids."""
        assert list(events.columns) == LOGIN_EVENT_COLUMNS
        assert list(events["event_id"]) == [
            f"E{i:07d}" for i in range(1, len(events) + 1)
        ]

    def test_accounts_starting_at_or_before_split_plus_one_are_skipped(self, events):
        """Accounts with no room before their first transaction get no events."""
        assert "C_SKIP_501" not in set(events["account_id"])
        assert "C_EDGE_502" in set(events["account_id"])

    def test_event_sequence_matches_login_scenario(self, events, accounts_with_devices):
        """Per account, event types appear in the order defined by its scenario."""
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
        """Events lie after the split and before the account's first transaction."""
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
        """SHORT_ACTIVITY events sit within the six hours before the transaction."""
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
        """Gaps between events respect the scenario maxima (120 s, 60 s, 3 h)."""

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
        """step and second_offset are derived consistently from event_time."""
        assert (events["step"] == events["event_time"] // HOUR + 1).all()
        assert (events["second_offset"] == events["event_time"] % HOUR).all()

    def test_device_profile_is_copied_and_renamed(self, events, accounts_with_devices):
        """Device attributes are copied from the account and renamed."""
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
        """Events are returned in chronological order."""
        assert events["event_time"].is_monotonic_increasing

    def test_account_with_one_hour_window_uses_that_hour(self, events):
        """An account with a one-hour window gets both events inside that hour."""
        times = events.loc[events["account_id"] == "C_EDGE_502", "event_time"]
        assert len(times) == 2
        assert ((times >= SPLIT_STEP * HOUR) & (times < (SPLIT_STEP + 1) * HOUR)).all()


def test_save_data_writes_three_parquet_files(
    tmp_path, monkeypatch, paysim_df, accounts_with_devices
):
    """save_data writes batch, stream and login parquet files with matching counts."""
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
    """Two runs under the project seed produce identical login events."""
    first = _run_generation(paysim_df, identity_df)
    second = _run_generation(paysim_df, identity_df)
    assert not first.empty
    pd.testing.assert_frame_equal(first, second)
