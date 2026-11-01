import pandas as pd
import os
import boto3

local_parquet_path = "data/preprocessed/paysim_batch.parquet"
object_name = "paysim/historical_paysim.parquet"
bucket_name = "historical-data"

s3_client = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_URL", "http://minio:9000"),
    aws_access_key_id=os.getenv("MINIO_ROOT_USER", "admin"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "password123"),
)

if os.path.exists(local_parquet_path):
    try:
        try:
            s3_client.head_bucket(Bucket=bucket_name)
        except Exception:
            s3_client.create_bucket(Bucket=bucket_name)

        s3_client.upload_file(local_parquet_path, bucket_name, object_name)

    except Exception as e:
        print(f"error occured: {e}")
else:
    print(f"File {local_parquet_path} does not exist. Run prepare_data.py first.")