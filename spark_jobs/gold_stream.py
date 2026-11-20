"""GOLD = business logic. Sales per category, rebuilt from Silver on every micro-batch."""
from pyspark.sql.functions import col, sum as _sum, count, avg, round as _round, max as _max, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType, TimestampType
from common import get_spark, SILVER_PATH, GOLD_PATH, CHECKPOINTS

spark = get_spark("gold")

silver_schema = StructType([
    StructField("invoice_ts", TimestampType()),
    StructField("category", StringType()),
    StructField("price", DoubleType()),
    StructField("kafka_partition", IntegerType()),
    StructField("kafka_offset", LongType()),
    StructField("processed_at", TimestampType()),
])

# the stream is only used as a "trigger": every time new Silver files appear we rebuild Gold
silver_stream = spark.readStream.schema(silver_schema).parquet(SILVER_PATH)

def build_gold(batch_df, batch_id):
    silver = spark.read.schema(silver_schema).parquet(SILVER_PATH)
    gold = (
        silver.groupBy("category")
        .agg(
            _round(_sum("price"), 2).alias("total_amount"),
            count("*").alias("invoice_count"),
            _round(avg("price"), 2).alias("average_price"),
            _max("invoice_ts").alias("last_invoice_ts"),
        )
        .withColumn("updated_at", current_timestamp())
    )
    gold.coalesce(1).write.mode("overwrite").parquet(GOLD_PATH)
    print(f"Gold rebuilt after batch {batch_id}")

(
    silver_stream.writeStream
    .foreachBatch(build_gold)
    .option("checkpointLocation", f"{CHECKPOINTS}/gold")
    .trigger(processingTime="15 seconds")
    .start()
    .awaitTermination()
)
