package com.knowyou.util


import org.apache.spark.sql.SparkSession

object testHbase {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder
//      .master("local[*]")
      .appName("testHbase")
      .enableHiveSupport
      .getOrCreate
    sparkSession.sparkContext.setLogLevel("DEBUG")

    val frame = sparkSession.sql("select device_id,video_id from knowyou_jituan_dmt.ads_rec_userdetail_wtr limit 5")

    frame.foreach(row => {
      val rowkey = row.getAs[String]("device_id")
      val value = row.getAs[String]("video_id")
      HbaseClient.putData("test", rowkey, "info", "video_id", value);
//      println(rowkey+"<---->"+value)
    })
  }
}
