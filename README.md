# Kafka -> Spark -> Bronze/Silver/Gold (HDFS) -> Hive  (all in Docker)

Replaces the "SQL Server" box of the diagram with a Kafka stream of invoices.

```
invoice_generator.py -> Kafka (topic "invoices")
   -> Spark Streaming -> BRONZE (raw)  -> SILVER (cleaned) -> GOLD (business logic)   [parquet on HDFS]
   -> Hive Metastore (Postgres) -> Beeline / HiveQL (BI developer)
   -> Spark / Jupyter (data scientist)
```

## Steps (run from this folder)

1. Start everything (first build takes a while, needs ~8 GB RAM free)
   docker compose up -d --build
   docker compose ps          # wait ~1-2 min until hiveserver2 is up

2. Create the Kafka topic
   docker exec broker /opt/kafka/bin/kafka-topics.sh --create --topic invoices \
     --bootstrap-server broker:29092 --partitions 3 --replication-factor 1

3. Start the producer on your host
   python3 -m venv stream && source stream/bin/activate && pip install kafka-python
   python3 producer/invoice_generator.py        # leave running (terminal 1)

4. Create the Hive tables (Bronze/Silver/Gold)
   docker exec hiveserver2 beeline -u jdbc:hive2://localhost:10000 -f /sql/init_tables.hql

5. Start the 3 streaming jobs (each in its own terminal, or with -d)
   PK=org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.7
   docker exec -d spark /opt/spark/bin/spark-submit --master local[2] --packages $PK /jobs/bronze_stream.py
   docker exec -d spark /opt/spark/bin/spark-submit --master local[2] /jobs/silver_stream.py
   docker exec -d spark /opt/spark/bin/spark-submit --master local[2] /jobs/gold_stream.py
   (drop the -d to see the logs of a job)

6. Check HDFS has the 3 layers
   docker exec namenode hdfs dfs -ls -R /lakehouse | head -30
   or open http://localhost:9870 -> Utilities -> Browse file system

7. Query with Beeline (BI developer)
   docker exec -it hiveserver2 beeline -u jdbc:hive2://localhost:10000
   > SELECT * FROM gold.category_sales;

8. Analyse (data analyst / scientist)
   Spark shell:  docker exec -it spark /opt/spark/bin/spark-shell \
        --conf spark.hadoop.hive.metastore.uris=thrift://hive-metastore:9083 \
        --conf spark.sql.catalogImplementation=hive
        scala> spark.sql("select * from gold.category_sales").show()
   Jupyter:      http://localhost:8888  -> open analysis.ipynb

9. Discover data: the metastore lives in Postgres
   docker exec -it metastore-db psql -U hive -d metastore -c 'select "DB_ID","NAME" from "DBS";'

Web UIs: Kafka UI http://localhost:9000 | HDFS http://localhost:9870 | HiveServer2 http://localhost:10002

Stop: docker compose down      (add -v to also delete all data)
