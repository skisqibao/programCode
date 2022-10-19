package com.qb.shanghai


import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, MapType, StringType, StructType}
import org.apache.spark.sql.functions.{explode, from_json}

case class PersonInfo(
                       age: Long,
                       birthday: String,
                       bplace: String,
                       hhplace: String,
                       idno: String,
                       idtype: String,
                       nation: String,
                       photo: String,
                       query_string: String,
                       rname: String,
                       sex: String
                     )

object person_info {
  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://172.16.1.220:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("person_info")

    val ss = SparkSession.builder().config(sparkConf).getOrCreate()
    ss.sparkContext.setLogLevel("ERROR")

    import ss.implicits._

    val frame = ss.read.json("D:\\a-sk\\win_download\\Compressed\\person_info.json")
    frame.printSchema()
    frame.select(
      $"_source.rname".alias("rname"),
      $"_source.age".alias("age"),
      $"_source.idno".alias("idno"),
      $"_source.birthday".alias("birthday"),
      $"_source.bplace".alias("bplace"),
      $"_source.edegree".alias("edegree"),
      $"_source.escu".alias("escu"),
      $"_source.height".alias("height"),
      $"_source.heavy".alias("heavy"),
      $"_source.hhplace".alias("hhplace"),
      $"_source.marr".alias("marr"),
      $"_source.sex".alias("sex"),
      $"_source.query_string".alias("query_string"),
      $"_source.std_address".alias("std_address")
    )
      .createOrReplaceTempView("person_info")

    val younggirl = ss.sql("select * from person_info where sex='女' and marr = '未婚' and age between 20 and 30 ")

    younggirl.write
      .format("csv")
      .option("path","./res")
      .save()


  }
}
