package com.qb.recommender

import java.net.InetAddress

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.transport.InetSocketTransportAddress
import org.elasticsearch.transport.client.PreBuiltTransportClient


case class Movie(mid: Int, name: String, descri: String, timelong: String, issue: String, shoot: String, language: String, genres: String, actors: String, directors: String)

case class Rating(uid: Int, mid: Int, score: Double, timestamp: Int)

case class Tag(uid: Int, mid: Int, tag: String, timestamp: Int)

/**
 * @param uri MongoDB连接
 * @param db  MongoDB数据库
 */
case class MongoConfig(uri: String, db: String)

/**
 * @param httpHosts      http主机列表，逗号分隔，客户端连接9200端口
 * @param transportHosts transport主机列表，逗号分隔，集群内部传输9300端口
 * @param index          需要操作的索引
 * @param clustername    集群名称，默认elasticsearch
 */
case class ESConfig(httpHosts: String, transportHosts: String, index: String, clustername: String)

object DataLoader {

  val MOVIE_DATA_PATH = "D:\\a-sk\\programCode\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH = "D:\\a-sk\\programCode\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH = "D:\\a-sk\\programCode\\MovieRecommendSystem\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"

  val MONGODB_MOVIE_COLLECTION = "Movie"
  val MONGODB_RATING_COLLECTION = "Rating"
  val MONGODB_TAG_COLLECTION = "Tag"
  val ES_MOVIE_INDEX = "Movie"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://172.16.1.220:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httpHosts" -> "172.16.1.220:9200",
      "es.transportHosts" -> "172.16.1.220:9300",
      "es.index" -> "recommender",
      "es.cluster.name" -> "sk-cluster"
    )

    val sparkConf = new SparkConf().setMaster(config("spark.cores")).setAppName("DataLoader")

    val ss = SparkSession.builder().config(sparkConf).getOrCreate()

    import ss.implicits._

    val movieRdd = ss.sparkContext.textFile(MOVIE_DATA_PATH)
    val movieDf = movieRdd.map(x => {
      val items = x.split("\\^")
      Movie(items(0).toInt, items(1).trim, items(2).trim, items(3).trim, items(4).trim, items(5).trim, items(6).trim, items(7).trim, items(8).trim, items(9).trim)
    }).toDF()

    val ratingRdd = ss.sparkContext.textFile(RATING_DATA_PATH)
    val ratingDf = ratingRdd.map(x => {
      val attr = x.split(",")
      Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toInt)
    }).toDF()

    val tagRdd = ss.sparkContext.textFile(TAG_DATA_PATH)
    val tagDf = tagRdd.map(x => {
      val attr = x.split(",")
      Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toInt)
    }).toDF()

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))

    storeDataToMongDB(movieDf, ratingDf, tagDf)

    import org.apache.spark.sql.functions._

    val newTag = tagDf.groupBy($"mid")
      .agg(concat_ws("|", collect_set($"tag")).as("tags"))
      .select("mid", "tags")

    val movieWithTagsDf = movieDf.join(newTag, Seq("mid"), "left")

    implicit val esConfig = ESConfig(config("es.httpHosts"), config("es.transportHosts"), config("es.index"), config("es.cluster.name"))

    storeDataToElasticSearch(movieWithTagsDf)

    ss.stop()
  }

  def storeDataToMongDB(movieDf: DataFrame, ratingDf: DataFrame, tagDf: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))

    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).dropCollection()

    movieDf.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_MOVIE_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    ratingDf.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_RATING_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    tagDf.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TAG_COLLECTION)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()

    // create index
    mongoClient(mongoConfig.db)(MONGODB_MOVIE_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_RATING_COLLECTION).createIndex(MongoDBObject("mid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("uid" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TAG_COLLECTION).createIndex(MongoDBObject("mid" -> 1))

    mongoClient.close()
  }

  def storeDataToElasticSearch(movieDf: DataFrame)(implicit esConfig: ESConfig): Unit = {
    val settings: Settings = Settings.builder().put("cluster.name", esConfig.clustername).build()

    System.setProperty("es.set.netty.runtime.available.processors", "false")
    val esClient = new PreBuiltTransportClient(settings)

    val REGEX_HOST_PORT = "(.+):(\\d+)".r

    esConfig.transportHosts.split(",").foreach {
      case REGEX_HOST_PORT(host: String, port: String) => {
        esClient.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(host), port.toInt))
      }
    }

    if (esClient.admin().indices().exists(new IndicesExistsRequest(esConfig.index))
      .actionGet()
      .isExists
    ) {
      esClient.admin().indices().delete(new DeleteIndexRequest(esConfig.index))
    }

    esClient.admin().indices().create(new CreateIndexRequest(esConfig.index))

    movieDf.write
      .option("es.nodes", esConfig.httpHosts)
      .option("es.http.timeout", "10m")
      .option("es.mapping.id","mid")
      .mode("overwrite")
      .format("org.elasticsearch.spark.sql")
      .save(esConfig.index + "/" + ES_MOVIE_INDEX)
  }

}
