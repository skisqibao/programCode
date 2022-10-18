package com.knowyou.task

import com.typesafe.scalalogging.Logger
import org.apache.spark.api.java.function.PairFunction
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.ALS
import org.apache.spark.mllib.recommendation.Rating
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{DataTypes, LongType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}


/**
 * 基于als算法模型的猜你喜欢推荐
 */
object ALSRecTask1 {
  val logger = Logger(this.getClass)

  case class Rating(userId: Int, movieId: Int, rating: Float)

  def parseRating(row: Row): Rating = {
    assert(row.size == 3)
    Rating(row.getLong(0).toInt, row.getLong(1).toInt, row.getDecimal(2).floatValue())
  }

//  def flatMapFun(row: Row): ={
//
//  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("ALSRecTask")
      //      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    import spark.implicits._

    //    val distDeviceId = spark.sql("select distinct deviceid  as deviceid from knowyou_ott_ods.ods_uba_cn_videoplayinfo limit 10")
    val distDeviceId = spark.sql("select distinct deviceid  as deviceid from knowyou_ott_dmt.dmt_rec_als_datasource_dm")

    // 在原Schema信息的基础上添加一列 “id”信息
    val distDevSchema: StructType = distDeviceId.schema.add(DataTypes.createStructField("id", DataTypes.LongType, false))
    // DataFrame转RDD 然后调用 zipWithIndex
    val distDeviceIdRDD = distDeviceId.rdd.zipWithIndex()
    val distDevRowRDD = distDeviceIdRDD.map(tp => Row.merge(tp._1, Row(tp._2)))
    // 将添加了索引的RDD 转化为DataFrame
    val df2 = spark.createDataFrame(distDevRowRDD, distDevSchema)
    df2.createTempView("device_dim")
    df2.show()

    val distVideoId = spark.sql("select distinct seriesheadcode  from knowyou_ott_dmt.dmt_rec_als_datasource_dm ")
    val distVideoSchema: StructType = distVideoId.schema.add(StructField("id", LongType))
    // DataFrame转RDD 然后调用 zipWithIndex
    val distVideoIdRDD = distVideoId.rdd.zipWithIndex()
    val distVideoRowRDD = distVideoIdRDD.map(tp => Row.merge(tp._1, Row(tp._2)))
    val df3 = spark.createDataFrame(distVideoRowRDD, distVideoSchema)
    df3.createTempView("video_dim")
    df3.show()


    val rawDataDF = spark.sql("select b.id as uid,c.id as pid,a.catalog," +
      " (case when a.playtime < 10 then 0.5 when a.playtime>=10 and a.playtime<=30 then 0.7 else 1.0 end) as rating " +
      "from( select deviceid,seriesheadcode,catalog,playtime from knowyou_ott_dmt.dmt_rec_als_datasource_dm )a " +
      "left join ( select * from device_dim )b on a.deviceid=b.deviceid " +
      "left join ( select * from video_dim )c on a.seriesheadcode=c.seriesheadcode ")
    rawDataDF.createTempView("raw_data")


    val ratingDF = spark.sql(String.format("select uid,pid,rating from raw_data where catalog='%s'", "体育"))
    val ratings = ratingDF.map(row => parseRating(row))
    val Array(training, test) = ratings.randomSplit(Array(0.8, 0.2))

    val rawDataCounts = rawDataDF.count()
    println(s"rawDataCounts count = $rawDataCounts")

    val ratingCounts = ratings.count()
    println(s"ratings count = $ratingCounts")
    // Build the recommendation model using ALS on the training data
    var ranks = Array(10)
    var regParams = Array(10)
    var alphas = Array(60)

    for (rank <- ranks) {
      for (regParam <- regParams) {
        for (alpha <- alphas) {

          val als = new ALS()
            .setMaxIter(10)
            .setRegParam(regParam)
            .setImplicitPrefs(true)
            .setRank(rank)
            .setAlpha(alpha)
            .setUserCol("userId")
            .setItemCol("movieId")
            .setRatingCol("rating")
          val model = als.fit(training)

          // Evaluate the model by computing the RMSE on the test data
          // Note we set cold start strategy to 'drop' to ensure we don't get NaN evaluation metrics
          model.setColdStartStrategy("drop")
          val predictions = model.transform(test)

          val evaluator = new RegressionEvaluator()
            .setMetricName("rmse")
            .setLabelCol("rating")
            .setPredictionCol("prediction")
          val rmse = evaluator.evaluate(predictions)
          println(s"rank : $rank ,regParam: $regParam ,alpha: $alpha ,Root-mean-square error = $rmse")

          // Generate top 10 movie recommendations for each user
          val userRecs = model.recommendForAllUsers(50)


          /*
          // Generate top 10 user recommendations for each movie
          val movieRecs = model.recommendForAllItems(10)

          // Generate top 10 movie recommendations for a specified set of users
          val users = ratings.select(als.getUserCol).distinct().limit(3)
          val userSubsetRecs = model.recommendForUserSubset(users, 10)
          // Generate top 10 user recommendations for a specified set of movies
          val movies = ratings.select(als.getItemCol).distinct().limit(3)
          val movieSubSetRecs = model.recommendForItemSubset(movies, 10)
          // $example off$
          userRecs.show()
          movieRecs.show()
          userSubsetRecs.show()
          movieSubSetRecs.show()*/



        }
      }
    }
    spark.close()

  }
}
