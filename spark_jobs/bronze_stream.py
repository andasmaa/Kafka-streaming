"""BRONZE = raw data. Kafka -> HDFS parquet, no cleaning at all."""
from pyspark.sql.functions import col, current_timestamp
from common import get_spark, BRONZE_PATH, CHECKPOINTS

spark = get_spark("bronze")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "broker:29092")   # internal listener (we are inside Docker)
    .option("subscribe", "invoices")
    .option("startingOffsets", "earliest")
    .load()
)

bronze = raw.select(
    col("value").cast("string").alias("raw_json"),
    col("topic").alias("kafka_topic"),
    col("partition").alias("kafka_partition"),
    col("offset").alias("kafka_offset"),
    col("timestamp").alias("kafka_timestamp"),
    current_timestamp().alias("ingested_at"),
)

(
    bronze.writeStream.format("parquet")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", f"{CHECKPOINTS}/bronze")
    .trigger(processingTime="10 seconds")
    .outputMode("append")
    .start()
    .awaitTermination()
)
