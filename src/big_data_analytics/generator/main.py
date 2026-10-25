import boto3

#connection to MinIO
s3_client = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='admin',
    aws_secret_access_key='password123'
)

bucket_name = 'historical-data'

#if the bucket doesn't exist, create it
try:
    s3_client.head_bucket(Bucket=bucket_name)
    print(f"Bucket '{bucket_name}' already exists.")
except:
    s3_client.create_bucket(Bucket=bucket_name)
    print(f"Created bucket: '{bucket_name}'.")