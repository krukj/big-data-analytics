import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as sf

logger = logging.getLogger(__name__)

class HistoricalPipeline:
    def __init__(self, spark):
        self.spark = spark
        self.data_path = "s3a://historical-data/paysim/historical_paysim.parquet"

    def read_data(self):
        return self.spark.read.parquet(self.data_path)

    def validate_data(self, df):
        logger.info("Starting historical data validation")

        required_columns = [
            "step", "type", "amount", "nameOrig",
            "oldbalanceOrg", "newbalanceOrig",
            "nameDest", "oldbalanceDest", "newbalanceDest",
            "isFraud", "isFlaggedFraud",
            "second_offset", "event_time"
        ]
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing columns: {missing_columns}")
        logger.info("Required columns validated")

        null_counts = df.select([
            sf.sum(sf.col(col).isNull().cast("int")).alias(col)
            for col in required_columns
        ]).first()
        logger.info("Null values: %s", null_counts.asDict())

        # total_rows = df.count()
        # unique_rows = df.distinct().count()
        # logger.info("Total rows: %s", total_rows)
        # logger.info("Duplicate rows: %s", total_rows - unique_rows)

        invalid_amount = df.filter(sf.col("amount") < 0).count()
        invalid_step = df.filter(
            (sf.col("step") < 1) | (sf.col("step") > 500)
        ).count()
        invalid_type = df.filter(
            ~sf.col("type").isin("PAYMENT", "TRANSFER", "CASH_OUT", "CASH_IN", "DEBIT")
        ).count()
        logger.info("Invalid amounts: %s", invalid_amount)
        logger.info("Invalid steps: %s", invalid_step)
        logger.info("Invalid transaction types: %s", invalid_type)

        step_range = df.select(
            sf.min("step").alias("min_step"),
            sf.max("step").alias("max_step")
        ).first()
        logger.info("Step range: %s - %s", step_range["min_step"], step_range["max_step"])

        logger.info("Historical data validation completed")

    def run(self):
        logger.info("Starting historical pipeline")

        df = self.read_data()
        self.validate_data(df)

        logger.info("Historical pipeline completed")

def main():
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
    )
    
    spark = (
        SparkSession.builder
        .appName("HistoricalPaySimPipeline")
        .master("spark://spark-master:7077")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "admin")
        .config("spark.hadoop.fs.s3a.secret.key", "password123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .getOrCreate()
    )

    try:
        pipeline = HistoricalPipeline(spark)
        pipeline.run()
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
