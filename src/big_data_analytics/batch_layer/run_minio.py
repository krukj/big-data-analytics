import logging
import os

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)
# connection to MinIO
s3_client = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_URL", "http://minio:9000"),
    aws_access_key_id=os.getenv("MINIO_ROOT_USER", "admin"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "password123"),
)
bucket_name = "historical-data"

# if the bucket doesn't exist, create it
try:
    s3_client.head_bucket(Bucket=bucket_name)
    logger.info(f"Bucket '{bucket_name}' already exists.")
except ClientError:
    s3_client.create_bucket(Bucket=bucket_name)
    logger.info(f"Created bucket: '{bucket_name}'.")
