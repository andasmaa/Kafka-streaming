-- Register the HDFS folders as Hive tables (this is what the Hive Metastore box does)
CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;

CREATE EXTERNAL TABLE IF NOT EXISTS bronze.invoices_raw (
  raw_json STRING,
  kafka_topic STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  kafka_timestamp TIMESTAMP,
  ingested_at TIMESTAMP
) STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/lakehouse/bronze/invoices';

CREATE EXTERNAL TABLE IF NOT EXISTS silver.invoices_clean (
  invoice_ts TIMESTAMP,
  category STRING,
  price DOUBLE,
  kafka_partition INT,
  kafka_offset BIGINT,
  processed_at TIMESTAMP
) STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/lakehouse/silver/invoices';

CREATE EXTERNAL TABLE IF NOT EXISTS gold.category_sales (
  category STRING,
  total_amount DOUBLE,
  invoice_count BIGINT,
  average_price DOUBLE,
  last_invoice_ts TIMESTAMP,
  updated_at TIMESTAMP
) STORED AS PARQUET
LOCATION 'hdfs://namenode:9000/lakehouse/gold/category_sales';
