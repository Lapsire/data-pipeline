import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import from_json, col, current_timestamp

# ENV
MYSQL_PWD = os.environ.get("MYSQL_ROOT_PASSWORD", "root")
MYSQL_DB = os.environ.get("MYSQL_DATABASE", "users")
KAFKA_TOPIC_DATA = os.environ.get("KAFKA_TOPIC_DATA", "users")
BATCH_INTERVAL = os.environ.get("BATCH_INTERVAL", "2 seconds")

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
    StructField("sent_at", TimestampType(), True),
    StructField("created_at", TimestampType(), True)
])

# Read users update topic from kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", KAFKA_TOPIC_DATA) \
    .option("startingOffsets", "earliest") \
    .load()

# Apply shape to the raw data from kafka
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), json_schema).alias("data")) \
    .select("data.*") \
    .withColumn("created_at", current_timestamp())
    
def write_to_mysql(batch_df, batch_id):
    """
    Write a data batch into the database.
    """
    batch_df.write \
        .format("jdbc") \
        .option("url", f"jdbc:mysql://mysql:3306/{MYSQL_DB}?useSSL=false&allowPublicKeyRetrieval=true") \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("dbtable", "users") \
        .option("user", "root") \
        .option("password", MYSQL_PWD) \
        .mode("append") \
        .save()

# Write data flux in db  
query = parsed_df.writeStream \
.foreachBatch(write_to_mysql) \
.trigger(processingTime=BATCH_INTERVAL) \
.start()

# Script will not stop until spark working
query.awaitTermination()