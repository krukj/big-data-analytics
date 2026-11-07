import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 10, 1),
    "retries": 1,
}

with DAG(
    "stream_to_minio",
    default_args=default_args,
    description="Consume messages from Kafka, clean the data, save to Minio",
    schedule=timedelta(minutes=1),
    catchup=False,
) as dag:

    @task
    def upload_streams_to_minio():
        sys.path.append("/app/src/big_data_analytics")
        from batch_layer.clean_stream_data import StreamConsumer

        consumer = StreamConsumer()
        consumer.run()

    upload_task = upload_streams_to_minio()
