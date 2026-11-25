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
    df = sample_df.drop("amount")

    with pytest.raises(ValueError, match="Missing columns") as error:
        pipeline.validate_data(df)
    print(f"column 'amount' dropped -> raised ValueError: {error.value}")


def test_engineer_features(pipeline, sample_df):
    result = pipeline.engineer_features(sample_df)

    row = result.filter("nameOrig = 'C1'").first()
    print(f"C1 amount_log: {row['amount_log']:.5f} (expected 4.61512)")
    print(
        f"C1 hour_of_day: {row['hour_of_day']} (expected 0), day: {row['day']} (expected 1)"
    )
    print(f"C1 orig_balance_diff: {row['orig_balance_diff']} (expected 100.0)")
    print(f"C1 dest_balance_diff: {row['dest_balance_diff']} (expected 100.0)")
    print(
        f"C1 amount_to_balance_ratio: {row['amount_to_balance_ratio']} (expected 0.2)"
    )

    assert row["amount_log"] == pytest.approx(4.61512)
    assert row["hour_of_day"] == 0
    assert row["day"] == 1
    assert row["orig_balance_diff"] == 100.0
    assert row["dest_balance_diff"] == 100.0
    assert row["amount_to_balance_ratio"] == pytest.approx(0.2)


def test_engineer_features_zero_balance(pipeline, sample_df):
    df = sample_df.withColumn("oldbalanceOrg", sample_df["oldbalanceOrg"] * 0)
    result = pipeline.engineer_features(df)

    ratios = [row["amount_to_balance_ratio"] for row in result.collect()]
    print(
        f"oldbalanceOrg set to 0 -> amount_to_balance_ratio per row: {ratios} (expected all None)"
    )
    assert all(ratio is None for ratio in ratios)


def test_aggregate_data(pipeline, sample_df):
    hourly_stats, _, type_stats = pipeline.aggregate_data(sample_df)
    hourly = hourly_stats.filter("step = 1").first()
    transfer = type_stats.filter("type = 'TRANSFER'").first()
    print(
        f"step 1: transaction_count={hourly['transaction_count']} (expected 2), "
        f"total_amount={hourly['total_amount']} (expected 150.0), "
        f"avg_amount={hourly['avg_amount']} (expected 75.0)"
    )
    print(
        f"TRANSFER: transaction_count={transfer['transaction_count']} (expected 2), "
        f"total_amount={transfer['total_amount']} (expected 300.0), "
        f"avg_amount={transfer['avg_amount']} (expected 150.0)"
    )

    assert hourly["transaction_count"] == 2
    assert hourly["total_amount"] == pytest.approx(150.0)
    assert hourly["avg_amount"] == pytest.approx(75.0)

    transfer = type_stats.filter("type = 'TRANSFER'").first()

    assert transfer["transaction_count"] == 2
    assert transfer["total_amount"] == pytest.approx(300.0)
    assert transfer["avg_amount"] == pytest.approx(150.0)
