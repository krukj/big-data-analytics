import json
import logging
import random
import time
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer

from config import (
    FIRST_STEP,
    KAFKA_BROKER,
    LATENCY,
    LOGIN_PARQUET,
    LOGIN_TOPIC,
    STREAM_PARQUET,
    TRANSACTION_TOPIC,
)

logger = logging.getLogger(__name__)

COLS = [
    "type",
    "amount",
    "nameOrig",
    "oldbalanceOrg",
    "newbalanceOrig",
    "nameDest",
    "oldbalanceDest",
    "newbalanceDest",
]


class StreamProducer:
    def __init__(
        self,
        data_path: str = STREAM_PARQUET,
        login_data_path: str = LOGIN_PARQUET,
        latency: int = LATENCY,
        first_step: int = FIRST_STEP,
        transaction_topic: str = TRANSACTION_TOPIC,
        login_topic: str = LOGIN_TOPIC,
    ):
        self.paysim_data_path = self._check_data_path(data_path)
        self.login_data_path = self._check_data_path(login_data_path)

        self.producer = Producer(
            {
                "bootstrap.servers": KAFKA_BROKER,
                "acks": "all",
            }
        )
        self.transaction_topic = transaction_topic
        self.login_topic = login_topic
        self.latency = latency
        self.current_step = first_step
        self.df = pd.read_parquet(self.paysim_data_path)
        self.login_df = pd.read_parquet(self.login_data_path)
        self.delayed_rows = {}

    @staticmethod
    def _check_data_path(data_path: str) -> Path:
        """Checks if file in data_path is a Parquet file"""
        path = Path(data_path)
        if path.suffix == ".parquet":
            return path.resolve()
        raise ValueError(f"File {data_path} is not a Parquet file")

    def _take_next_stream(self, df) -> pd.DataFrame:
        """Returns filtered dataframe of"""
        return df[df["step"] == self.current_step]

    def _data_corruption(self, df: pd.DataFrame) -> pd.DataFrame:
        """Corrupts rows with some probability"""
        df = df.copy()
        rows_to_duplicate = []
        indices_to_drop = []
        for idx, row in df.iterrows():
            if random.random() < 0.02:
                random_col = random.choice(COLS)
                df.at[idx, random_col] = None

            if random.random() < 0.005:
                rows_to_duplicate.append(row)

            if random.random() < 0.01:
                self.delayed_rows[self.current_step].append(row)
                indices_to_drop.append(idx)
        df = df.drop(index=indices_to_drop)
        if rows_to_duplicate:
            df = pd.concat([df, pd.DataFrame(rows_to_duplicate)], ignore_index=True)
        return df

    def produce(self):
        while True:
            self.delayed_rows[self.current_step] = []
            logger.info(f"Current step: {self.current_step}")
            current_stream = self._take_next_stream(self.df)
            current_logins = self._take_next_stream(self.login_df)
            if current_stream.empty:
                logger.info("No more data in the dataset. Stopping.")
                break

            df_corrupted = self._data_corruption(current_stream)

            for _, row in current_logins.iterrows():
                clean_row = row.where(pd.notna(row), None).to_dict()
                self.producer.produce(
                    topic=self.login_topic, value=json.dumps(clean_row)
                )

            for _, row in df_corrupted.iterrows():
                clean_row = row.where(pd.notna(row), None).to_dict()
                self.producer.produce(
                    topic=self.transaction_topic, value=json.dumps(clean_row)
                )

            if current_delayed := self.delayed_rows.get(self.current_step - 1, []):
                for dr in current_delayed:
                    clean_row = dr.where(pd.notna(dr), None).to_dict()
                    self.producer.produce(
                        topic=self.transaction_topic, value=json.dumps(clean_row)
                    )

            self.producer.flush()
            logger.info(
                f"Produced transactions stream of length {len(df_corrupted) + len(current_delayed)} to {self.transaction_topic}"
            )
            logger.info(
                f"Produced logins stream of length {len(current_logins)} to {self.login_topic}"
            )
            time.sleep(self.latency)
            self.current_step += 1


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    stream_producer = StreamProducer()
    stream_producer.produce()
