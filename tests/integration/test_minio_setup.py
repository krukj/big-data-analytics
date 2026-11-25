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
    """Verify run_minio.py created every bucket the pipelines write to.

    Steps:
    1. List the MinIO buckets with boto3.
    Expected:
    - historical-data, clean-transactions-data, clean-logins-data and processed-data
        exist.
    """
    names = {bucket["Name"] for bucket in s3.list_buckets()["Buckets"]}
    print(f"buckets in MinIO: {sorted(names)}")
    print(f"missing buckets: {sorted(EXPECTED_BUCKETS - names) or 'none'}")
    assert EXPECTED_BUCKETS <= names


def test_historical_dataset_is_uploaded(s3):
    """Verify the historical PaySim parquet is uploaded to MinIO.

    Steps:
    1. Request the metadata (head_object) of paysim/historical_paysim.parquet in the
        historical-data bucket.
    Expected:
    - The object exists.
    - Its size is greater than 0 bytes.
    """
    try:
        head = s3.head_object(
            Bucket=MINIO_BUCKET_BATCH, Key=MINIO_HISTORICAL_OBJECT_NAME
        )
    except ClientError as error:
        pytest.fail(f"historical object missing: {error}")
    print(f"object {MINIO_BUCKET_BATCH}/{MINIO_HISTORICAL_OBJECT_NAME} exists")
    print(f"size: {head['ContentLength']} bytes (expected > 0)")
    assert head["ContentLength"] > 0
