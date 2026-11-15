import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as sf

from config import (
    MINIO_PASSWORD,
    MINIO_USER,
)

logger = logging.getLogger(__name__)

class HistoricalPipeline:
    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark
        self.data_path = "s3a://historical-data/paysim/historical_paysim.parquet"

    def read_data(self) -> DataFrame:
        return self.spark.read.parquet(self.data_path)

    def validate_data(self, df: DataFrame):
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

    def engineer_features(self, df: DataFrame) -> DataFrame:
        logger.info("Starting feature engineering")

        df = (
            df
            .withColumn("amount_log", sf.log1p(sf.col("amount")))
            .withColumn("hour_of_day", (sf.col("step") - 1) % 24)
            .withColumn("day", sf.floor((sf.col("step") - 1) / 24) + 1)
            .withColumn("orig_balance_diff", sf.col("oldbalanceOrg") - sf.col("newbalanceOrig"))
            .withColumn("dest_balance_diff", sf.col("newbalanceDest") - sf.col("oldbalanceDest"))
            .withColumn("amount_to_balance_ratio",
                sf.when(
                    sf.col("oldbalanceOrg") > 0,
                    sf.col("amount") / sf.col("oldbalanceOrg")
                ).otherwise(None)
            )
        )

        logger.info("Feature engineering completed")
        return df

    def aggregate_data(self, df: DataFrame) -> tuple[DataFrame, DataFrame, DataFrame]:
        logger.info("Starting historical data aggregation")

        hourly_stats = (
            df.groupBy("step")
            .agg(
                sf.count("*").alias("transaction_count"),
                sf.sum("amount").alias("total_amount"),
                sf.avg("amount").alias("avg_amount"),
                sf.expr("percentile_approx(amount, 0.5)").alias("median_amount"),
                sf.max("amount").alias("max_amount")
            )
            .orderBy("step")
        )

        hourly_type_stats = (
            df.groupBy("step", "type")
            .agg(
                sf.count("*").alias("transaction_count"),
                sf.sum("amount").alias("total_amount"),
                sf.avg("amount").alias("avg_amount")
            )
            .orderBy("step", "type")
        )

        type_stats = (
            df.groupBy("type")
            .agg(
                sf.count("*").alias("transaction_count"),
                sf.sum("amount").alias("total_amount"),
                sf.avg("amount").alias("avg_amount")
            )
            .orderBy("type")
        )

        logger.info("Historical data aggregation completed")
        return hourly_stats, hourly_type_stats, type_stats

    def save_data(
        self, 
        df: DataFrame, 
        hourly_stats: DataFrame, 
        hourly_type_stats: DataFrame, 
        type_stats: DataFrame,
    ) -> None:
        logger.info("Saving processed historical data")

        base_path = "s3a://processed-data/paysim"
        df.write.mode("overwrite").parquet(f"{base_path}/transactions/")
        hourly_stats.write.mode("overwrite").parquet(f"{base_path}/hourly_stats/")
        hourly_type_stats.write.mode("overwrite").parquet(f"{base_path}/hourly_type_stats/")
        type_stats.write.mode("overwrite").parquet(f"{base_path}/type_stats/")
        logger.info("Processed historical data saved")

    def run(self):
        logger.info("Starting historical pipeline")

        df = self.read_data()
        self.validate_data(df)
        df = self.engineer_features(df)
        hourly_stats, hourly_type_stats, type_stats = self.aggregate_data(df)
        self.save_data(df, hourly_stats, hourly_type_stats, type_stats)

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
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
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
