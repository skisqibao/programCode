from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("kafka-rw") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0") \
    .getOrCreate()

line = spark.readStream \
    .format("socket") \
    .option("host", "172.16.1.220") \
    .option("port", "9997") \
    .load() \
    .select("value") \
    .writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "./filesink") \
    .option("checkpointLocation", "./ck4") \
    .start() \
    .awaitTermination()
