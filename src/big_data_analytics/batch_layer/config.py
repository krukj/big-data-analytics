import os
from pathlib import Path

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

REPO_ROOT = Path(os.getenv("REPO_ROOT", Path(__file__).resolve().parents[3]))
# data
RAW_KAGGLE_DIR = REPO_ROOT / "data" / "raw_kaggle"
RAW_STREAM_DIR = REPO_ROOT / "data" / "raw_stream"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"

# files
PAYSIM_RAW_FILE = RAW_KAGGLE_DIR / "paysim_data.csv"
IDENTITY_RAW_FILE = RAW_KAGGLE_DIR / "train_identity.csv"

BATCH_PARQUET = PROCESSED_DIR / "paysim_batch.parquet"
STREAM_PARQUET = RAW_STREAM_DIR / "paysim_stream.parquet"
LOGIN_PARQUET = RAW_STREAM_DIR / "login_events.parquet"

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TRANSACTION_TOPIC = os.getenv("TRANSACTION_TOPIC", "raw-transactions")
LOGIN_TOPIC = os.getenv("LOGIN_TOPIC", "raw-logins")
LATENCY = int(os.getenv("LATENCY", "10"))
FIRST_STEP = int(os.getenv("FIRST_STEP", "501"))

MINIO_URL = os.getenv("MINIO_URL", "http://localhost:9000")
MINIO_USER = os.getenv("MINIO_ROOT_USER", "admin")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD", "password123")
MINIO_BUCKET_BATCH = "historical-data"
MINIO_HISTORICAL_OBJECT_NAME = "paysim/historical_paysim.parquet"
BUCKET_NAMES = [
    "historical-data",
    "raw-transactions-data",
    "raw-logins-data",
    "clean-transactions-data",
    "clean-logins-data",
    "processed-data",
]
SPARK_CONFIG = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4"
)

TRANSACTIONS_SCHEMA = StructType(
    [
        StructField("step", IntegerType()),
        StructField("type", StringType()),
        StructField("amount", DoubleType()),
        StructField("nameOrig", StringType()),
        StructField("oldbalanceOrg", DoubleType()),
        StructField("newbalanceOrig", DoubleType()),
        StructField("nameDest", StringType()),
        StructField("oldbalanceDest", DoubleType()),
        StructField("newbalanceDest", DoubleType()),
    ]
)

LOGINS_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("account_id", StringType()),
        StructField("device_id", StringType()),
        StructField("event_type", StringType()),
        StructField("step", IntegerType()),
        StructField("second_offset", IntegerType()),
        StructField("event_time", IntegerType()),
        StructField("device_type", StringType()),
        StructField("operating_system", StringType()),
        StructField("browser", StringType()),
        StructField("device_info", StringType()),
        StructField("screen_resolution", StringType()),
    ]
)
