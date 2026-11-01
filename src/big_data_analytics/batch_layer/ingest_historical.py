import pandas as pd
import os
import io
import boto3

local_csv_path = "data/raw/paysim_data.csv"
object_name = "paysim/historical_paysim.parquet"
bucket_name = "historical-data"

s3_client = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_URL", "http://minio:9000"),
    aws_access_key_id=os.getenv("MINIO_ROOT_USER", "admin"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "password123"),
)

if os.path.exists(local_csv_path):
    try:
        df = pd.read_csv(local_csv_path)
        if "step" in df.columns:
            df_history = df[df["step"] <= 500]
        else:
            df_history = df

        #df to parquet
        parquet_buffer = io.BytesIO()
        df_history.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        print(f"Uploading {object_name} to bucket {bucket_name}...")
        s3_client.upload_fileobj(parquet_buffer, bucket_name, object_name)

    except Exception as e:
        print(f"error occured: {e}")