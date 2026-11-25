# this file will be deleted, only cleaning part wil be used

import json
import logging
import tempfile
from datetime import datetime

import boto3
import pandas as pd
from confluent_kafka import Consumer

from .config import (
    KAFKA_BROKER,
    LOGIN_TOPIC,
    MINIO_PASSWORD,
    MINIO_URL,
    MINIO_USER,
    TRANSACTION_TOPIC,
)

logger = logging.getLogger(__name__)


class StreamConsumer:
    consumer = Consumer(
        {
            "bootstrap.servers": KAFKA_BROKER,
            "group.id": "stream-cleaner-group",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    consumer.subscribe(topics=[TRANSACTION_TOPIC, LOGIN_TOPIC])

    def consume(self, first_poll_timeout=120, idle_timeout=3):
        transactions = []
        logins = []
        timeout = first_poll_timeout
        while True:
            message = self.consumer.poll(timeout=timeout)
            if message is None:
                logger.info("No new messages")
                break
            if message.error():
                logger.error(f"Kafka error: {message.error()}")
                continue
            logger.info("Found correct message")
            timeout = idle_timeout
            data = json.loads(message.value().decode())
            topic = message.topic()
            if topic == TRANSACTION_TOPIC:
                transactions.append(data)
            elif topic == LOGIN_TOPIC:
                logins.append(data)
        return transactions, logins

    def data_cleaning(self, data: list):
        df = pd.DataFrame(data)
        df = df.drop_duplicates()

        for col in ("amount", "oldbalanceOrg", "newbalanceOrig"):
            if col in df.columns:
                df[col] = df[col].astype("float64")

        if "amount" in df.columns:
            mask_amount_null = df["amount"].isnull()
            df.loc[mask_amount_null, "amount"] = abs(
                df.loc[mask_amount_null, "oldbalanceOrg"]
                - df.loc[mask_amount_null, "newbalanceOrig"]
            )

        if "oldbalanceOrg" in df.columns:
            mask_oldbalance_null = df["oldbalanceOrg"].isnull()
            mask_out = mask_oldbalance_null & df["type"].isin(
                ["CASH_OUT", "PAYMENT", "TRANSFER"]
            )

            df.loc[mask_out, "oldbalanceOrg"] = (
                df.loc[mask_out, "newbalanceOrig"] + df.loc[mask_out, "amount"]
            )

            mask_in = mask_oldbalance_null & (df["type"] == "CASH_IN")
            df.loc[mask_in, "oldbalanceOrg"] = (
                df.loc[mask_in, "newbalanceOrig"] - df.loc[mask_in, "amount"]
            )

        if "newbalanceOrig" in df.columns:
            mask_newbalance_null = df["newbalanceOrig"].isnull()
            mask_out = mask_newbalance_null & df["type"].isin(
                ["CASH_OUT", "PAYMENT", "TRANSFER"]
            )

            df.loc[mask_out, "newbalanceOrig"] = (
                df.loc[mask_out, "oldbalanceOrg"] - df.loc[mask_out, "amount"]
            )

            mask_in = mask_newbalance_null & (df["type"] == "CASH_IN")
            df.loc[mask_in, "newbalanceOrig"] = (
                df.loc[mask_in, "oldbalanceOrg"] + df.loc[mask_in, "amount"]
            )

        return df

    def upload_to_minio(self, df: pd.DataFrame, prefix: str, bucket: str):
        if df.empty:
            return

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{prefix}_{timestamp}.parquet"

        s3_client = boto3.client(
            "s3",
            endpoint_url=MINIO_URL,
            aws_access_key_id=MINIO_USER,
            aws_secret_access_key=MINIO_PASSWORD,
        )

        with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp_file:
            df.to_parquet(tmp_file.name, index=False)

            s3_client.upload_file(Filename=tmp_file.name, Bucket=bucket, Key=filename)
        logger.info(f"Uploaded {filename} to bucket {bucket}")

    def run(self):
        transactions, logins = self.consume()

        if not transactions and not logins:
            logger.info("No data")
            return

        if transactions:
            transactions_clean = self.data_cleaning(transactions)
            self.upload_to_minio(
                transactions_clean, "clean_transactions", "clean-transactions-data"
            )

        if logins:
            logins_clean = self.data_cleaning(logins)
            self.upload_to_minio(logins_clean, "clean_logins", "clean-logins-data")

        self.consumer.commit()
        logger.info("Stream data succesfully loaded to Minio")
