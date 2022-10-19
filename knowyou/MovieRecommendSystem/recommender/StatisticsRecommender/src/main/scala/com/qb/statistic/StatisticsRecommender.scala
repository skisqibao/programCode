package com.qb.statistic

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String, shoot: String, language: String, genres: String, actors: String, directors: String)

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class MongoConfig(uri: String, db: String)

case class Recommendation(mid: Int, score: Double)

case class GenresRecommendation(genres: String, recs: Seq[Recommendation])

object StatisticsRecommender {
  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"

  //统计的表的名称
  val RATE_MORE_MOVIES = "RateMoreMovies"
  val RATE_MORE_RECENTLY_MOVIES = "RateMoreRecentlyMovies"
  val AVERAGE_MOVIES = "AverageMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://172.16.1.220:27017/recommender",
      "mongo.db" -> "recommender"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("StatisticsRecommender")

    val ss = SparkSession.builder().config(sparkConf).getOrCreate()

    import ss.implicits._

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    val ratingDf = ss.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
      .toDF()

    val movieDf = ss.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]
      .toDF()

    ratingDf.createOrReplaceTempView("ratings")
    movieDf.createOrReplaceTempView("movies")

    // 历史热门统计，历史评分数据最多
    val rateMoreMovidesDF = ss.sql("select mid, count(mid) as cnt from ratings group by mid")
    storeDfToMongDB(rateMoreMovidesDF, RATE_MORE_MOVIES)

    // 近期热门统计，按照‘yyyyMM’格式选取最近的评分数据，统计个数
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    ss.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x * 1000L)).toInt)

    val ratingOfYearMonth = ss.sql("select mid, score, changeDate(timestamp) as yearmonth from ratings")
    ratingOfYearMonth.createOrReplaceTempView("ratingOfMonth")

    val rateMoreRecentlyMoviesDf = ss.sql("select mid, count(mid) as cnt, yearmonth from ratingOfMonth group by yearmonth, mid order by yearmonth desc, cnt desc")
    storeDfToMongDB(rateMoreRecentlyMoviesDf, RATE_MORE_RECENTLY_MOVIES)

    // 优质电影推荐，统计电影的平均评分
    val averageMoviesDf = ss.sql("select mid, avg(score) as avg from ratings group by mid")
    storeDfToMongDB(averageMoviesDf, AVERAGE_MOVIES)

    // 各类别电影top统计
    val genres = List("Action", "Adventure", "Animation", "Comedy", "Crime", "Documentary", "Drama", "Family", "Fantasy", "Foreign", "History", "Horror", "Music", "Mystery", "Romance", "Science", "Tv", "Thriller", "War", "Western")

    val movieWithScore = movieDf.join(averageMoviesDf, "mid")

    val genresRdd = ss.sparkContext.makeRDD(genres)

    val genresTopMoviesDf = genresRdd.cartesian(movieWithScore.rdd)
      .filter {
        case (genre, row) => row.getAs[String]("genres").toLowerCase().contains(genre.toLowerCase)
      }
      .map {
        case (genre, row) => (genre, (row.getAs[Int]("mid"), row.getAs[Double]("avg")))
      }
      .groupByKey()
      .map {
        case (genre, items) => GenresRecommendation(genre, items.toList.sortWith(_._2 > _._2).take(10).map { item =>
          Recommendation(item._1, item._2)
        })
      }
      .toDF()

    storeDfToMongDB(genresTopMoviesDf, GENRES_TOP_MOVIES)

    ss.stop()
  }

  def storeDfToMongDB(df: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    df.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}
