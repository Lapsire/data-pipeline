import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import from_json, col, current_timestamp, expr, avg, min, max, lit, count

# ENV
MONGO_USER = os.getenv("MONGODB_USER", "root")
MONGO_PWD = os.getenv("MONGODB_PASSWORD", "root")
MONGO_DB = os.getenv("MONGODB_DATABASE", "users")
KAFKA_TOPIC_DATA = os.getenv("KAFKA_TOPIC_DATA", "users")
KAFKA_TOPIC_METRICS = os.getenv("KAFKA_TOPIC_METRICS", "metrics")
BATCH_INTERVAL = os.getenv("BATCH_INTERVAL", "2 seconds")
BATCH_ROWS_LIMIT = int(os.getenv("BATCH_ROW_LIMIT", "100000"))
INTERVAL_SECONDS = float(BATCH_INTERVAL.split(" ")[0])

def process_batch(batch_df, batch_id):

    batch_df.cache()
    
    # Count the batch size
    batch_size = batch_df.count()

    if batch_size > 0 :
        batch_df.drop("latency_seconds").write \
            .format("mongodb") \
            .option("spark.mongodb.write.connection.uri", f"mongodb://{MONGO_USER}:{MONGO_PWD}@mongodb:27017/?authSource=admin") \
            .option("database", MONGO_DB) \
            .option("collection", "users") \
            .mode("append") \
            .save()

        # Compute metrics
        metrics_df = batch_df.select(
            count("*").alias("count"),
            min("latency_seconds").alias("min_latency"),
            max("latency_seconds").alias("max_latency"),
            avg("latency_seconds").alias("avg_latency")
        ).withColumn("throughput", col("count") / lit(INTERVAL_SECONDS))

        # Send metrics
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
    .option("maxOffsetsPerTrigger", BATCH_ROWS_LIMIT) \
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
.option("checkpointLocation", "/tmp/spark_checkpoints_mongo") \
.trigger(processingTime=BATCH_INTERVAL) \
.start()

# Script will not stop while spark is working
spark.streams.awaitAnyTermination()