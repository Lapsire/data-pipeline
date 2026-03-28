import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.functions import expr

NUM_ROWS = os.getenv("NUM_ROWS", 1000000)
NUM_SHARDS = int(os.getenv("NUM_SHARDS", 100))

spark = SparkSession.builder \
    .appName("producer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN") # To get a clean terminal

df = spark.range(0, NUM_ROWS, 1, numPartitions=NUM_SHARDS)

generated_df = df.select(
    expr("concat('User_', id)").alias("name"),
    expr("cast(rand() * 82 + 18 as int)").alias("age"),
    expr("concat(name, '@example.com')").alias("email"),
    expr("uuid()").alias("password"),
    expr("element_at(array('Paris', 'Nice', 'Toulon', 'Lyon'), cast(rand() * 4 + 1 as int))").alias("city"),
    expr("current_timestamp()").alias("sent_at")
)

generated_df.write \
    .mode("overwrite") \
    .json("/data/users")

