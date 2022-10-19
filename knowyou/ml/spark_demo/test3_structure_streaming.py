from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import window

spark = SparkSession \
    .builder \
    .appName("StructuredNetworkWordCount") \
    .getOrCreate()

lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "172.16.1.220") \
    .option("port", "9997") \
    .load()

# words = lines.select(
#     lines.timestamp,
#     explode(
#         split(lines.value, " ")
#     ).alias("word")
# )
# wordCounts = words.groupBy("word").count()

lines.isStreaming()
lines.printSchema()

userSchema = StructType().add("timestamp", "timestamp").add("word", "string")

wordCounts = lines.groupBy(
    window(lines.timestamp, "10 minutes", "5 minutes"),
    lines.word
).count()

query = wordCounts \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()
