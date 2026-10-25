import json
import logging
import os
import random
import time
from pathlib import Path

import pandas as pd
from confluent_kafka import Producer

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


class PaySimStreamProducer:
    def __init__(
        self,
        data_path: str,
        latency: int = 10,
        first_step: int = 500,
        topic: str = "raw-transactions",
    ):
        self.data_path = self._check_data_path(data_path)
        self.producer = Producer(
            {
                "bootstrap.servers": os.getenv("KAFKA_BROKER", "localhost:9092"),
                "acks": "all",
            }
        )
        self.topic = topic
        self.latency = latency
        self.current_step = first_step
        self.df = pd.read_csv(self.data_path)
        self.delayed_rows = {}

    @staticmethod
    def _check_data_path(data_path: str) -> Path:
        """Checks if file in data_path is a CSV file"""
        path = Path(data_path)
        if path.suffix == ".csv":
            return path.resolve()
        raise ValueError(f"File {data_path} is not a CSV file")

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
            if current_stream.empty:
                logger.info("No more data in the dataset. Stopping.")
                break

            df_corrupted = self._data_corruption(current_stream)

            for _, row in df_corrupted.iterrows():
                clean_row = row.where(pd.notna(row), None).to_dict()
                self.producer.produce(topic=self.topic, value=json.dumps(clean_row))

            if current_delayed := self.delayed_rows.get(self.current_step - 1, []):
                for dr in current_delayed:
                    clean_row = dr.where(pd.notna(dr), None).to_dict()
                    self.producer.produce(topic=self.topic, value=json.dumps(clean_row))

            self.producer.flush()
            logger.info(
                f"Produced stream of length {len(df_corrupted) + len(current_delayed)}"
            )
            time.sleep(self.latency)
            self.current_step += 1


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    stream_producer = PaySimStreamProducer(data_path=os.getenv("DATA_PATH", "/app/data/raw/paysim_for_streams.csv"))
    stream_producer.produce()
