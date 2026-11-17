"""
Deployment checks for the MinIO storage setup (batch_layer/run_minio.py).

Require:  docker compose up -d minio batch-layer
"""

import pytest
from botocore.exceptions import ClientError

from config import MINIO_BUCKET_BATCH, MINIO_HISTORICAL_OBJECT_NAME

pytestmark = pytest.mark.integration

EXPECTED_BUCKETS = {
    "historical-data",
    "clean-transactions-data",
    "clean-logins-data",
    "processed-data",
}


def test_required_buckets_exist(s3):
    """run_minio.py created every bucket the pipelines write to."""
    names = {bucket["Name"] for bucket in s3.list_buckets()["Buckets"]}
    assert EXPECTED_BUCKETS <= names


def test_historical_dataset_is_uploaded(s3):
    """The historical PaySim parquet is in place and not empty."""
    try:
        head = s3.head_object(
            Bucket=MINIO_BUCKET_BATCH, Key=MINIO_HISTORICAL_OBJECT_NAME
        )
    except ClientError as error:
        pytest.fail(f"historical object missing: {error}")
    assert head["ContentLength"] > 0
