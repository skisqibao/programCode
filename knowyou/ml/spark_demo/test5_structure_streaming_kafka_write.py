from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .master("local[*]") \
    .appName("kafka-rw") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.16.1.222:6667") \
    .option("subscribe", "test") \
    .load()

out = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .outputMode("append") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "172.16.1.222:6667") \
    .option("topic", "test2") \
    .option("checkpointLocation", "./ck1") \
    .start() \
    .awaitTermination()
