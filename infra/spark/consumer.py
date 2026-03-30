import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import from_json, col, current_timestamp, expr, window, count, avg, min, max

# ENV
MYSQL_PWD = os.environ.get("MYSQL_ROOT_PASSWORD", "root")
MYSQL_DB = os.environ.get("MYSQL_DATABASE", "users")
KAFKA_TOPIC_DATA = os.environ.get("KAFKA_TOPIC_DATA", "users")
KAFKA_TOPIC_METRICS = os.getenv("KAFKA_TOPIC_METRICS", "metrics")
BATCH_INTERVAL = os.environ.get("BATCH_INTERVAL", "2 seconds")

def process_batch(batch_df, batch_id):

    batch_df.cache()
    
    # Count the batch size
    count = batch_df.count()

    if count > 0 :
        batch_df.drop("latency_seconds").write \
            .format("jdbc") \
            .option("url", f"jdbc:mysql://mysql:3306/{MYSQL_DB}?useSSL=false&allowPublicKeyRetrieval=true&rewriteBatchedStatements=true") \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("dbtable", "users") \
            .option("user", "root") \
            .option("password", MYSQL_PWD) \
            .option("batchsize", "10000") \
            .mode("append") \
            .save()

        # Count latency for processing
        latency_metrics = batch_df.select(
            min("latency_seconds").alias("min_latency"),
            max("latency_seconds").alias("max_latency"),
            avg("latency_seconds").alias("avg_latency")
        ).collect()[0]

        # Troughput calcul
        interval_sec = int(BATCH_INTERVAL.split(" ")[0]) 
        throughput = count / interval_sec

        # Send metrics to kafka
        metrics = [(
            int(count), 
            float(throughput), 
            float(latency_metrics["min_latency"]),
            float(latency_metrics["max_latency"]),
            float(latency_metrics["avg_latency"])
        )]

        column = ["count", "throughput", "min_latency", "max_latency", "avg_latency"]
        metrics_df = spark.createDataFrame(metrics, column)

        metrics_df.selectExpr("to_json(struct(*)) as value").write \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "kafka:29092") \
            .option("topic", KAFKA_TOPIC_METRICS) \
            .save()
    
    batch_df.unpersist()

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
    .withColumn("latency_seconds", expr("cast(created_at as double) - cast(sent_at as double)"))

# Write data flux in db  
query = parsed_df.writeStream \
.foreachBatch(process_batch) \
.option("checkpointLocation", "/tmp/spark_checkpoints_mysql") \
.trigger(processingTime=BATCH_INTERVAL) \
.start()

# Script will not stop while spark is working
spark.streams.awaitAnyTermination()