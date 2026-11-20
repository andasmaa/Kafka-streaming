from pyspark.sql import SparkSession

HDFS = "hdfs://namenode:9000"
BRONZE_PATH = f"{HDFS}/lakehouse/bronze/invoices"
SILVER_PATH = f"{HDFS}/lakehouse/silver/invoices"
GOLD_PATH   = f"{HDFS}/lakehouse/gold/category_sales"
CHECKPOINTS = f"{HDFS}/lakehouse/_checkpoints"

def get_spark(name):
    spark = (
        SparkSession.builder.appName(name)
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.hadoop.fs.defaultFS", HDFS)
        .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore:9083")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark
