"""SILVER = cleaned data. Reads Bronze, parses JSON, fixes types, drops bad rows."""
from pyspark.sql.functions import col, from_json, to_timestamp, initcap, trim, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, TimestampType
from common import get_spark, BRONZE_PATH, SILVER_PATH, CHECKPOINTS

spark = get_spark("silver")

bronze_schema = StructType([
    StructField("raw_json", StringType()),
    StructField("kafka_topic", StringType()),
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
    StructField("kafka_timestamp", TimestampType()),
    StructField("ingested_at", TimestampType()),
])

invoice_schema = StructType([
    StructField("timestamp", StringType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
])

bronze = spark.readStream.schema(bronze_schema).parquet(BRONZE_PATH)

silver = (
    bronze
    .withColumn("data", from_json(col("raw_json"), invoice_schema))
    .select(
        to_timestamp(col("data.timestamp"), "yyyy-MM-dd HH:mm:ss").alias("invoice_ts"),
        initcap(trim(col("data.category"))).alias("category"),
        col("data.price").alias("price"),
        col("kafka_partition"),
        col("kafka_offset"),
        current_timestamp().alias("processed_at"),
    )
    # data quality rules
    .filter(col("invoice_ts").isNotNull() & col("category").isNotNull() & (col("price") > 0))
)

(
    silver.writeStream.format("parquet")
    .option("path", SILVER_PATH)
    .option("checkpointLocation", f"{CHECKPOINTS}/silver")
    .trigger(processingTime="10 seconds")
    .outputMode("append")
    .start()
    .awaitTermination()
)
