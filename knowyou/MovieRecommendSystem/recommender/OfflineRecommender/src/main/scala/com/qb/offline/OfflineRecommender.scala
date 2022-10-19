package com.qb.offline

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

case class MovieRating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

case class Recommendation(mid: Int, score: Double)

case class UserRecommendation(uid: Int, recs: Seq[Recommendation])

case class MovieRecommendation(mid: Int, recs: Seq[Recommendation])


object OfflineRecommender {
  val MONGODB_RATING_COLLECTION = "Rating"

  val USER_RECS = "UserRecs"
  val MOVIE_RECS = "MovieRecs"
  val USER_MAX_RECOMMENDATION = 20

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://172.16.1.220:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("OfflineRecommender")

    val ss = SparkSession.builder().config(sparkConf).getOrCreate()

    import ss.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    val ratingRdd = ss.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[MovieRating]
      .rdd
      .map(rating => (rating.uid, rating.mid, rating.score))
      .cache()

    val userRdd = ratingRdd.map(_._1).distinct()
    val movieRdd = ratingRdd.map(_._2).distinct()


    ss.stop()
  }
}
