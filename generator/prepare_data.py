import logging

import numpy as np
import pandas as pd

from config import (
    IDENTITY_RAW_FILE,
    PAYSIM_RAW_FILE,
    PROCESSED_DIR,
    RANDOM_SEED,
    RAW_STREAM_DIR,
    SPLIT_STEP,
)

NEAR_WINDOW_HOURS = 6

DEVICE_COLUMNS = ["DeviceType", "id_30", "id_31", "DeviceInfo", "id_33"]
LOGIN_SCENARIOS = {
    "NORMAL_LOGIN": ["LOGIN_SUCCESS"],
    "SINGLE_FAILURE": ["LOGIN_FAILED", "LOGIN_SUCCESS"],
    "MULTIPLE_FAILURES": [
        "LOGIN_FAILED",
        "LOGIN_FAILED",
        "LOGIN_FAILED",
        "LOGIN_SUCCESS",
    ],
    "MULTIPLE_LOGINS": ["LOGIN_SUCCESS", "LOGIN_SUCCESS"],
}

logger = logging.getLogger(__name__)


def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Loads raw PaySim and IEEE-CIS identity datasets.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: PaySim transactions and IEEE-CIS identity records.
    """
    paysim_df = pd.read_csv(PAYSIM_RAW_FILE)
    identity_df = pd.read_csv(IDENTITY_RAW_FILE, usecols=DEVICE_COLUMNS)
    return paysim_df, identity_df


def prepare_paysim(
    paysim_df: pd.DataFrame, split_step: int = SPLIT_STEP
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Adds synthetic transaction timestamps and split PaySim into batch and streaming datasets.

    Arguments:
        - paysim_df (pd.DataFrame): Original PaySim transaction dataset.
        - split_step (int):Last simulation step included in the batch dataset.

    Returns:
        tuple[pd.DataFrame, pd.DataFrame]: Historical batch transactions and streaming transactions with second_offset and event_time columns.
    """
    paysim_df = paysim_df.copy()
    paysim_df["second_offset"] = 0
    paysim_df["event_time"] = (paysim_df["step"] - 1) * 3600
    batch_df = paysim_df.loc[paysim_df["step"] <= split_step].copy()
    stream_df = paysim_df.loc[paysim_df["step"] > split_step].copy()

    return batch_df, stream_df


def select_accounts(
    stream_df: pd.DataFrame, n_accounts: int | None = None
) -> pd.DataFrame:
    """
    Selects streaming accounts and determine their first transaction step.

    Arguments:
        - stream_df (pd.DataFrame): Streaming PaySim transactions.
        - n_accounts (int | None): Number of accounts to sample, or None to select all accounts.

    Returns:
        pd.DataFrame: Selected accounts with first_transaction_step and transaction_count.
    """
    selected_accounts = stream_df["nameOrig"].drop_duplicates()

    if n_accounts is not None:
        selected_accounts = selected_accounts.sample(
            n=n_accounts,
            random_state=RANDOM_SEED,
        )

    stream_subset = stream_df[stream_df["nameOrig"].isin(selected_accounts)].copy()

    account_info = (
        stream_subset.groupby("nameOrig")
        .agg(
            first_transaction_step=("step", "min"),
            transaction_count=("step", "size"),
        )
        .reset_index()
        .rename(columns={"nameOrig": "account_id"})
    )

    return account_info


def sample_device_profiles(
    identity_df: pd.DataFrame,
    account_info: pd.DataFrame,
) -> pd.DataFrame:
    """
    Sampls complete IEEE-CIS device profiles for selected accounts preserving the original distribution of known
    device types while accounting for profile frequencies within each device type..

    Arguments:
        - identity_df (pd.DataFrame): IEEE-CIS identity records.
        - account_info (pd.DataFrame): Selected PaySim accounts with transaction information.

    Returns:
        pd.DataFrame: Account information enriched with sampled device profiles and unique device identifiers.
    """
    complete = identity_df.dropna(subset=DEVICE_COLUMNS)
    if complete.empty:
        raise ValueError("No complete IEEE-CIS device profiles available")

    profiles = complete.groupby(DEVICE_COLUMNS).size().reset_index(name="count")
    device_type_probs = identity_df["DeviceType"].value_counts(normalize=True)
    conditional_probs = profiles["count"] / profiles.groupby("DeviceType")[
        "count"
    ].transform("sum")
    weights = profiles["DeviceType"].map(device_type_probs) * conditional_probs
    weights = weights.to_numpy(dtype=float, copy=True)
    weights /= weights.sum()

    selected_indices = np.random.choice(
        len(profiles),
        size=len(account_info),
        replace=True,
        p=weights,
    )
    sampled = profiles.iloc[selected_indices][DEVICE_COLUMNS].reset_index(drop=True)
    accounts = pd.concat([account_info.reset_index(drop=True), sampled], axis=1)
    accounts["device_id"] = [f"D{i:06d}" for i in range(1, len(accounts) + 1)]

    return accounts


def assign_login_scenarios(accounts_with_devices: pd.DataFrame) -> pd.DataFrame:
    """
    Assigns an activity pattern and login scenario to each account.

    Arguments:
        - accounts_with_devices (pd.DataFrame): Selected accounts with assigned device profiles.

    Returns:
        pd.DataFrame: Accounts enriched with activity_pattern and login_scenario.
    """
    accounts_with_devices = accounts_with_devices.copy()

    accounts_with_devices["activity_pattern"] = np.random.choice(
        ["SHORT_ACTIVITY", "MIXED_ACTIVITY"],
        size=len(accounts_with_devices),
        p=[0.7, 0.3],
    )

    accounts_with_devices["login_scenario"] = np.random.choice(
        list(LOGIN_SCENARIOS.keys()),
        size=len(accounts_with_devices),
        p=[0.65, 0.20, 0.05, 0.10],
    )

    return accounts_with_devices


def generate_login_events(
    accounts_with_devices: pd.DataFrame, split_step: int = SPLIT_STEP
) -> pd.DataFrame:
    """
    Generate synthetic login sequences before each account's first streaming transaction.
    - SHORT_ACTIVITY events occur within six hours before the transaction.
    - MIXED_ACTIVITY events are usually near the transaction but can occur earlier in the streaming period.

    Arguments:
        - accounts_with_devices (pd.DataFrame): Accounts with device profiles, activity patterns and login scenarios.
        - split_step (int):Last historical PaySim step.

    Returns:
        pd.DataFrame: Chronologically sorted login events with timestamps, event types and device profiles.
    """
    events = []

    earliest_event_time = split_step * 3600

    for _, account in accounts_with_devices.iterrows():
        first_step = int(account["first_transaction_step"])
        if first_step <= split_step + 1:
            continue

        scenario = account["login_scenario"]
        event_types = LOGIN_SCENARIOS[scenario]

        transaction_start = (first_step - 1) * 3600
        near_start = max(
            earliest_event_time, transaction_start - NEAR_WINDOW_HOURS * 3600
        )

        if account["activity_pattern"] == "SHORT_ACTIVITY":
            sequence_window_start = near_start
            sequence_window_end = transaction_start

        else:
            is_near = np.random.choice([True, False], p=[0.8, 0.2])

            if is_near or near_start == earliest_event_time:
                sequence_window_start = near_start
                sequence_window_end = transaction_start
            else:
                sequence_window_start = earliest_event_time
                sequence_window_end = near_start

        available_duration = sequence_window_end - sequence_window_start

        if scenario == "NORMAL_LOGIN":
            gaps = []

        elif scenario == "SINGLE_FAILURE":
            max_gap = min(120, available_duration - 1)
            gaps = [np.random.randint(1, max_gap + 1)]

        elif scenario == "MULTIPLE_FAILURES":
            max_gap = min(60, (available_duration - 1) // 3)

            if max_gap < 1:
                continue

            gaps = list(np.random.randint(1, max_gap + 1, size=3))

        else:  # MULTIPLE_LOGINS
            max_gap = min(3 * 3600, available_duration - 1)
            gaps = [np.random.randint(1, max_gap + 1)]

        total_duration = sum(gaps)

        last_login_time = np.random.randint(
            sequence_window_start + total_duration,
            sequence_window_end,
        )

        first_event_time = last_login_time - total_duration
        event_times = [first_event_time]

        for gap in gaps:
            event_times.append(event_times[-1] + gap)

        for event_order, (event_time, event_type) in enumerate(
            zip(event_times, event_types)
        ):
            events.append(
                {
                    "account_id": account["account_id"],
                    "device_id": account["device_id"],
                    "activity_pattern": account["activity_pattern"],
                    "login_scenario": scenario,
                    "event_type": event_type,
                    "event_order": event_order,
                    "event_time": event_time,
                }
            )

    account_events = pd.DataFrame(events)

    account_events["step"] = account_events["event_time"] // 3600 + 1
    account_events["second_offset"] = account_events["event_time"] % 3600

    account_events = account_events.sort_values(
        ["event_time", "account_id", "event_order"]
    ).reset_index(drop=True)

    generated_events = account_events.merge(
        accounts_with_devices[["account_id"] + DEVICE_COLUMNS],
        on="account_id",
        how="left",
    )

    generated_events["event_id"] = [
        f"E{i:07d}" for i in range(1, len(generated_events) + 1)
    ]

    generated_events = generated_events[
        [
            "event_id",
            "account_id",
            "device_id",
            "event_type",
            "step",
            "second_offset",
            "event_time",
            "DeviceType",
            "id_30",
            "id_31",
            "DeviceInfo",
            "id_33",
        ]
    ].copy()

    generated_events = generated_events.rename(
        columns={
            "DeviceType": "device_type",
            "id_30": "operating_system",
            "id_31": "browser",
            "DeviceInfo": "device_info",
            "id_33": "screen_resolution",
        }
    )

    return generated_events


def save_data(
    batch_df: pd.DataFrame,
    stream_df: pd.DataFrame,
    generated_events: pd.DataFrame,
) -> None:
    """
    Saves prepared PaySim transactions and synthetic login events.

    Arguments:
        - batch_df (pd.DataFrame): Historical PaySim transactions.
        - stream_df (pd.DataFrame): Streaming PaySim transactions with synthetic timestamps.
        - generated_events (pd.DataFrame): Synthetic login events with device information.

    Returns:
        None: Saves three Parquet files to the preprocessed data directory.
    """
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    RAW_STREAM_DIR.mkdir(parents=True, exist_ok=True)

    batch_df.to_parquet(PROCESSED_DIR / "paysim_batch.parquet", index=False)
    stream_df.to_parquet(RAW_STREAM_DIR / "paysim_stream.parquet", index=False)
    generated_events.to_parquet(RAW_STREAM_DIR / "login_events.parquet", index=False)


def main() -> None:
    """
    Prepares PaySim datasets and generate streaming login events.
    """
    np.random.seed(RANDOM_SEED)
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    paysim_df, identity_df = load_data()
    batch_df, stream_df = prepare_paysim(paysim_df, SPLIT_STEP)
    account_info = select_accounts(stream_df)
    accounts_with_devices = sample_device_profiles(identity_df, account_info)
    accounts_with_devices = assign_login_scenarios(accounts_with_devices)
    generated_events = generate_login_events(accounts_with_devices, SPLIT_STEP)
    save_data(batch_df, stream_df, generated_events)

    logger.info(f"Batch transactions: {len(batch_df)}")
    logger.info(f"Streaming transactions: {len(stream_df)}")
    logger.info(f"Accounts with synthetic activity: {len(account_info)}")
    logger.info(f"Generated login events: {len(generated_events)}")
    logger.info(f"Saved files to: {PROCESSED_DIR}")


if __name__ == "__main__":
    main()
