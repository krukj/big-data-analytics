import pytest
from batch_layer.historical_pipeline import HistoricalPipeline
from pyspark.sql import SparkSession


@pytest.fixture(scope="module")
def spark():
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("HistoricalPipelineTests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )

    yield spark
    spark.stop()


@pytest.fixture
def pipeline(spark):
    return HistoricalPipeline(spark)


@pytest.fixture
def sample_df(spark):
    data = [
        (
            1,
            "TRANSFER",
            100.0,
            "C1",
            500.0,
            400.0,
            "C2",
            200.0,
            300.0,
            0,
            0,
            120,
            "2026-01-01 00:02:00",
        ),
        (
            1,
            "PAYMENT",
            50.0,
            "C3",
            100.0,
            50.0,
            "M1",
            0.0,
            0.0,
            0,
            0,
            180,
            "2026-01-01 00:03:00",
        ),
        (
            2,
            "TRANSFER",
            200.0,
            "C4",
            1000.0,
            800.0,
            "C5",
            300.0,
            500.0,
            0,
            0,
            60,
            "2026-01-01 01:01:00",
        ),
    ]

    columns = [
        "step",
        "type",
        "amount",
        "nameOrig",
        "oldbalanceOrg",
        "newbalanceOrig",
        "nameDest",
        "oldbalanceDest",
        "newbalanceDest",
        "isFraud",
        "isFlaggedFraud",
        "second_offset",
        "event_time",
    ]

    return spark.createDataFrame(data, columns)


def test_validate_data_missing_columns(pipeline, sample_df):
    """Verify validation rejects a frame that lacks a required column.

    Steps:
    1. Drop the amount column from the three-row sample frame.
    2. Call validate_data on the reduced frame.
    Expected:
    - ValueError whose message starts with "Missing columns" is raised.
    """
    df = sample_df.drop("amount")

    with pytest.raises(ValueError, match="Missing columns"):
        pipeline.validate_data(df)


def test_engineer_features(pipeline, sample_df):
    """Verify engineered features are computed from the raw columns as specified.

    Steps:
    1. Call engineer_features on the sample frame.
    2. Read the row of account C1 (TRANSFER of 100 from balance 500 to 400).
    Expected:
    - amount_log equals log(1 + 100) = 4.61512.
    - hour_of_day is 0 and day is 1 for event_time 2026-01-01 00:02:00.
    - orig_balance_diff and dest_balance_diff both equal 100.0.
    - amount_to_balance_ratio equals 100 / 500 = 0.2.
    """
    result = pipeline.engineer_features(sample_df)

    row = result.filter("nameOrig = 'C1'").first()

    assert row["amount_log"] == pytest.approx(4.61512)
    assert row["hour_of_day"] == 0
    assert row["day"] == 1
    assert row["orig_balance_diff"] == 100.0
    assert row["dest_balance_diff"] == 100.0
    assert row["amount_to_balance_ratio"] == pytest.approx(0.2)


@pytest.mark.quality
def test_engineer_features_zero_balance(pipeline, sample_df):
    """Verify a zero originating balance yields a null ratio, not a division error.

    Steps:
    1. Set oldbalanceOrg to 0 in every row of the sample frame.
    2. Call engineer_features and collect amount_to_balance_ratio.
    Expected:
    - amount_to_balance_ratio is null in every row (no infinity, no exception).
    """
    df = sample_df.withColumn("oldbalanceOrg", sample_df["oldbalanceOrg"] * 0)
    result = pipeline.engineer_features(df)

    assert all(row["amount_to_balance_ratio"] is None for row in result.collect())


@pytest.mark.quality
def test_aggregate_data(pipeline, sample_df):
    """Verify per-step and per-type aggregates conserve counts and amounts.

    Steps:
    1. Call aggregate_data on the sample frame (two rows in step 1, one in step 2).
    2. Read the step-1 row of hourly_stats and the TRANSFER row of type_stats.
    Expected:
    - Step 1: transaction_count 2, total_amount 150.0, avg_amount 75.0.
    - TRANSFER: transaction_count 2, total_amount 300.0, avg_amount 150.0.
    """
    hourly_stats, _, type_stats = pipeline.aggregate_data(sample_df)
    hourly = hourly_stats.filter("step = 1").first()

    assert hourly["transaction_count"] == 2
    assert hourly["total_amount"] == pytest.approx(150.0)
    assert hourly["avg_amount"] == pytest.approx(75.0)

    transfer = type_stats.filter("type = 'TRANSFER'").first()

    assert transfer["transaction_count"] == 2
    assert transfer["total_amount"] == pytest.approx(300.0)
    assert transfer["avg_amount"] == pytest.approx(150.0)
