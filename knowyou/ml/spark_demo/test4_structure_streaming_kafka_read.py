from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

import time

time.sleep(240)

spark = SparkSession \
    .builder \
    .appName("kafka-test") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.16.1.222:6667") \
    .option("subscribe", "test") \
    .load()

words = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)", "topic", "partition", "offset", "timestamp",
                      "timestamptype")

words.printSchema

query = words \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
