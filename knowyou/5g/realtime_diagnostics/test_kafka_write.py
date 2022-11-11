# _*_coding:utf-8_*_
from pyspark.sql import SparkSession


if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master("local[*]") \
        .appName("test kafka write") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.0") \
        .getOrCreate()

    data = [('A', 18), ('B', 20)]
    df = spark.createDataFrame(data, ['name', 'age'])
    print(df.collect())
