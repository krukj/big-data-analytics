import logging

import boto3
from botocore.exceptions import ClientError

from config import (
    BATCH_PARQUET,
    MINIO_HISTORICAL_OBJECT_NAME,
    MINIO_PASSWORD,
    MINIO_URL,
    MINIO_USER,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
# connection to MinIO
s3_client = boto3.client(
    "s3",
    endpoint_url=MINIO_URL,
    aws_access_key_id=MINIO_USER,
    aws_secret_access_key=MINIO_PASSWORD,
)
bucket_names = ["historical-data", "clean-transactions-data", "clean-logins-data"]

for bn in bucket_names:
    try:
        s3_client.head_bucket(Bucket=bn)
        logger.info(f"Bucket '{bn}' already exists.")
    except ClientError:
        s3_client.create_bucket(Bucket=bn)
        logger.info(f"Created bucket: '{bn}'.")
    if bn == "historical-data":
        try:
            s3_client.upload_file(
                Filename=str(BATCH_PARQUET), Bucket=bn, Key=MINIO_HISTORICAL_OBJECT_NAME
            )
        except ValueError as e:
            logger.error(f"Error during uploading file to Silo: {e}")
