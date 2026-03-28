import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import from_json, col, current_timestamp, expr, window, count, avg, struct

# ENV
MYSQL_PWD = os.environ.get("MYSQL_ROOT_PASSWORD", "root")
MYSQL_DB = os.environ.get("MYSQL_DATABASE", "users")
KAFKA_TOPIC_DATA = os.environ.get("KAFKA_TOPIC_DATA", "users")
KAFKA_TOPIC_METRICS = os.getenv("KAFKA_TOPIC_METRICS", "metrics")
BATCH_INTERVAL = os.environ.get("BATCH_INTERVAL", "2 seconds")

def write_to_mysql(batch_df, batch_id):
    """
    Write a data batch into the database.
    """
    batch_df.drop("latency_seconds").write \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://mysql:3306/{MYSQL_DB}?useSSL=false&allowPublicKeyRetrieval=true") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "users") \
        .option("user", "root") \
        .option("password", MYSQL_PWD) \
        .mode("append") \
        .save()

spark = SparkSession.builder\
    .appName("consumer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN") # To get a clean terminal

# Data shape
json_schema = StructType([
    StructField("name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("email", StringType(), True),
    StructField("password", StringType(), True),
    StructField("city", StringType(), True),
    StructField("sent_at", TimestampType(), True)
])

# Read users update topic from kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", KAFKA_TOPIC_DATA) \
    .option("startingOffsets", "earliest") \
    .load()

# Apply shape to the raw data from kafka
parsed_df = df.selectExpr("cast(value as string)") \
    .select(from_json(col("value"), json_schema).alias("data")) \
    .select("data.*") \
    .withColumn("created_at", current_timestamp()) \
    .withColumn("latency_seconds", expr("unix_timestamp(created_at) - unix_timestamp(sent_at)"))

# Compute metrics
metrics_df = parsed_df \
    .withWatermark("sent_at", "10 seconds") \
    .groupBy(
        window(col("sent_at"), "10 seconds"), 
        col("city")
    ) \
    .agg(
        count("*").alias("volume"), 
        avg("latency_seconds").alias("avg_latency")
    )

# Transform metrics df for kafka
kafka_metrics_df = metrics_df.selectExpr(
        "cast(window.start as string) as key",
        "to_json(struct(*)) as value"
    )

query_metrics = kafka_metrics_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("topic", KAFKA_TOPIC_METRICS) \
    .option("checkpointLocation", "/tmp/spark_checkpoints_metrics") \
    .outputMode("update") \
    .start()

# Write data flux in db  
query = parsed_df.writeStream \
.foreachBatch(write_to_mysql) \
.trigger(processingTime=BATCH_INTERVAL) \
.start()

# Script will not stop while spark is working
spark.streams.awaitAnyTermination()