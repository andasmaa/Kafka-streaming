from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, from_json, to_timestamp, initcap, trim, current_timestamp, sum as _sum, count, avg, round as _round, max as _max)
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, LongType, IntegerType, TimestampType)

HDFS = "hdfs://namenode:9000"
BRONZE = f"{HDFS}/lakehouse/bronze/invoices"
SILVER = f"{HDFS}/lakehouse/silver/invoices"
GOLD = f"{HDFS}/lakehouse/gold/category_sales"
CKPT = f"{HDFS}/lakehouse/_checkpoints"

spark = SparkSession.builder.appName("lakehouse").config("spark.sql.shuffle.partitions", "2").config("spark.hadoop.fs.defaultFS", HDFS).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

bronze_schema = StructType([StructField("raw_json", StringType()), StructField("kafka_topic", StringType()), StructField("kafka_partition", IntegerType()), StructField("kafka_offset", LongType()), StructField("kafka_timestamp", TimestampType()), StructField("ingested_at", TimestampType())])
silver_schema = StructType([StructField("invoice_ts", TimestampType()), StructField("category", StringType()), StructField("price", DoubleType()), StructField("kafka_partition", IntegerType()), StructField("kafka_offset", LongType()), StructField("processed_at", TimestampType())])
invoice_schema = StructType([StructField("timestamp", StringType()), StructField("category", StringType()), StructField("price", DoubleType())])

raw = spark.readStream.format("kafka").option("kafka.bootstrap.servers", "broker:29092").option("subscribe", "invoices").option("startingOffsets", "earliest").load()
bronze = raw.select(col("value").cast("string").alias("raw_json"), col("topic").alias("kafka_topic"), col("partition").alias("kafka_partition"), col("offset").alias("kafka_offset"), col("timestamp").alias("kafka_timestamp"), current_timestamp().alias("ingested_at"))
q1 = bronze.writeStream.format("parquet").option("path", BRONZE).option("checkpointLocation", CKPT + "/bronze").trigger(processingTime="10 seconds").outputMode("append").start()
print(">>> BRONZE started")

b = spark.readStream.schema(bronze_schema).parquet(BRONZE)
parsed = b.withColumn("data", from_json(col("raw_json"), invoice_schema))
silver = parsed.select(to_timestamp(col("data.timestamp"), "yyyy-MM-dd HH:mm:ss").alias("invoice_ts"), initcap(trim(col("data.category"))).alias("category"), col("data.price").alias("price"), col("kafka_partition"), col("kafka_offset"), current_timestamp().alias("processed_at")).filter(col("invoice_ts").isNotNull() & col("category").isNotNull() & (col("price") > 0))
q2 = silver.writeStream.format("parquet").option("path", SILVER).option("checkpointLocation", CKPT + "/silver").trigger(processingTime="10 seconds").outputMode("append").start()
print(">>> SILVER started")

def build_gold(batch_df, batch_id):
    s = spark.read.schema(silver_schema).parquet(SILVER)
    g = s.groupBy("category").agg(_round(_sum("price"), 2).alias("total_amount"), count("*").alias("invoice_count"), _round(avg("price"), 2).alias("average_price"), _max("invoice_ts").alias("last_invoice_ts")).withColumn("updated_at", current_timestamp())
    g.coalesce(1).write.mode("overwrite").parquet(GOLD)
    print(">>> GOLD rebuilt after batch " + str(batch_id))

s_stream = spark.readStream.schema(silver_schema).parquet(SILVER)
q3 = s_stream.writeStream.foreachBatch(build_gold).option("checkpointLocation", CKPT + "/gold").trigger(processingTime="20 seconds").start()
print(">>> GOLD started")

spark.streams.awaitAnyTermination()
