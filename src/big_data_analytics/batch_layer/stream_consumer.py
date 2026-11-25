from pyspark.sql import SparkSession  # noqa: I001
from pyspark.sql.functions import col, from_json

from batch_layer.config import (  # type: ignore
    KAFKA_BROKER,
    LOGIN_TOPIC,
    LOGINS_SCHEMA,
    MINIO_PASSWORD,
    MINIO_URL,
    MINIO_USER,
    SPARK_CONFIG,
    TRANSACTION_TOPIC,
    TRANSACTIONS_SCHEMA,
)


def consume():

    spark = (
        SparkSession.builder.appName("StreamConsumer")
        .config("spark.jars.packages", SPARK_CONFIG)
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_URL)
        .config("spark.hadoop.fs.s3a.access.key", MINIO_USER)
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_PASSWORD)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )

    df_transactions = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", TRANSACTION_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
        .select(
            from_json(col("value").cast("string"), TRANSACTIONS_SCHEMA).alias("data")
        )
        .select("data.*")
    )
    df_logins = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", LOGIN_TOPIC)
        .option("startingOffsets", "earliest")
        .load()
        .select(from_json(col("value").cast("string"), LOGINS_SCHEMA).alias("data"))
        .select("data.*")
    )

    parq_transactions = (
        df_transactions.writeStream.format("parquet")
        .outputMode("append")
        .partitionBy("step", "type")
        .option("path", "s3a://raw-transactions-data/")
        .option("checkpointLocation", "s3a://raw-transactions-data/_checkpoints/")
        .trigger(availableNow=True)
        .start()
    )

    parq_logins = (
        df_logins.writeStream.format("parquet")
        .outputMode("append")
        .partitionBy("step")
        .option("path", "s3a://raw-logins-data/")
        .option("checkpointLocation", "s3a://raw-logins-data/_checkpoints/")
        .trigger(availableNow=True)
        .start()
    )

    parq_transactions.awaitTermination()
    parq_logins.awaitTermination()
    spark.stop()
