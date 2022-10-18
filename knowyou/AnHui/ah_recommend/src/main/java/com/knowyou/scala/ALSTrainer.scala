package com.knowyou.scala

import java.time.{LocalDate, LocalDateTime}

import breeze.numerics.sqrt
import com.knowyou.Scheduler.ALSDataPretreat
import org.apache.spark.SparkConf
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD}
import org.apache.spark.mllib.recommendation.{ALS, MatrixFactorizationModel, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.slf4j.LoggerFactory

import scala.collection.mutable.ArrayBuffer

case class Param(
                  curType: String,
                  rank: Int,
                  iteration: Int,
                  lambda: Double,
                  alpha: Double,
                  rmse: Double,
                  precision: Double,
                  recall: Double,
                  f1score: Double,
                  auc: Double,
                  dt: String
                )

object ALSTrainer {
  val logger = LoggerFactory.getLogger(ALSTrainer.getClass)

  def main(args: Array[String]): Unit = {
    val config = Map(
      "spark.cores" -> "local[*]"
    )

    val sparkConf = new SparkConf()
      .setAppName("ALSTrainer")
    //      .setMaster(config("spark.cores"))

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val paidMap = Map(
      "free" -> " and package_series ='99' ",
      "pay" -> " and package_series !='99' ",
      "all" -> " "
    )

    //    val categoryArr = Array("电视剧", "电影", "少儿", "综艺")
    val categoryArr = Array("电视剧", "综艺")
    val paidArr = Array("pay")

    for (category <- categoryArr; paid <- paidArr) {
      val typeBroadcast = spark.sparkContext.broadcast(paid + category)
      // 加载源数据
      val alsPreTreatData = new ALSDataPretreat(category, paidMap(paid))
      val trainDataMap = alsPreTreatData.getTrainData(spark)

      val trainDataRdd = trainDataMap.get("trainDataRDD")
        .asInstanceOf[JavaRDD[Rating]]
        .rdd
      //        .randomSplit(Array(0.4, 0.6))(0)

      println("origin train rdd partitions: " + trainDataRdd.partitions.size)
      val stringLongVideoRdd = trainDataMap.get("stringLongVideoPairRDD").asInstanceOf[JavaPairRDD[String, Long]].rdd

      val trainDataSize = trainDataRdd.count()
      var trainingRdd: RDD[Rating] = null
      var testingRdd: RDD[Rating] = null

      println(trainDataSize)
      var ratio: Double = 0.2
      if (trainDataSize * ratio > 20000) {
        ratio = (20000 / trainDataSize).toDouble
        if (ratio < 0.1) ratio = 0.1
        val splits1 = trainDataRdd.randomSplit(Array(1 - ratio, ratio))

        val testingTmpRdd = splits1(1)
        val count = testingTmpRdd.count
        println("origin test set size: " + count)
        if (count > 30000) {
          ratio = (20000.0 / count.toDouble).formatted("%.2f").toDouble

          val splits2 = testingTmpRdd.randomSplit(Array(1 - ratio, ratio))
          println("Second split ratio: " + ratio)

          trainingRdd = splits1(0)
            .union(splits2(0))
          testingRdd = splits2(1)
        } else {
          trainingRdd = splits1(0)
          testingRdd = splits1(1)
        }
      } else {
        val splits3 = trainDataRdd.randomSplit(Array(1 - ratio, ratio))
        trainingRdd = splits3(0)
        testingRdd = splits3(1)
      }

      println("split train rdd partitions: " + testingRdd.partitions.size)
      println("split test rdd partitions: " + testingRdd.partitions.size)
      testingRdd.coalesce(150).cache()

      val trainingCount = trainingRdd.count()
      val testingCount = testingRdd.count()

      println(trainingCount)
      println(testingCount)
      if (testingCount != 0) {
        // user交集
        val intersectionUserRdd = trainingRdd.map(_.user)
          .distinct()
          .intersection(testingRdd.map(_.user).distinct())

        // product交集
        val intersectionProductRdd = trainingRdd.map(_.product)
          .distinct()
          .intersection(testingRdd.map(_.product).distinct())

        val validUserArray = intersectionUserRdd.collect()
        val validProductArray = intersectionProductRdd.collect()

        val finalTestingRdd = testingRdd
          .filter(x => validUserArray.contains(x.user) && validProductArray.contains(x.product))

        val finalTrainingRdd = testingRdd
          .filter(x => !(validUserArray.contains(x.user) && validProductArray.contains(x.product)))
          .union(trainingRdd)

        println("切分train集数：" + trainingCount)
        println("切分test 集数：" + testingCount)
        println("user   交集数：" + validUserArray.length)
        println("product交集数：" + validProductArray.length)
        println("最终train集数：" + finalTrainingRdd.count())
        println("最终test 集数：" + finalTestingRdd.count())

        println("final train partition size: " + finalTrainingRdd.partitions.size)
        println("final test  partition size: " + finalTestingRdd.getNumPartitions)
        val result = adjustALSParams(spark, finalTrainingRdd, finalTestingRdd, stringLongVideoRdd)

        testingRdd.unpersist()

        val resultRdd = spark.sparkContext.parallelize(result)
        val curType = typeBroadcast.value
        val s = resultRdd.map(x => Param(curType, x._1, x._2, x._3, x._4, x._5, x._6, x._7, x._8, x._9, LocalDate.now().toString))
          .toDS

        println("Max f1score show: ")
        s.orderBy($"f1score".desc)
          .show(100)

        println("Min rmse show: ")
        s.orderBy($"rmse".asc)
          .show(100)

        try {
          s.write
            .mode("overwrite")
            .partitionBy("dt")
            .saveAsTable("knowyou_ott_dmt.guess_like_adjust_params_res")
        } catch {
          case e: Exception =>
        }

        /*        try {
                  s.write
                    .format("csv")
                    .mode("overwrite")
                    .save("file:///root/sk/params")
                } catch {
                  case e: Exception =>
                }*/
      }
    }

    spark.stop()
  }

  def adjustALSParams(spark: SparkSession, trainingRdd: RDD[Rating], testingRdd: RDD[Rating], stringLongVideoRdd: RDD[(String, Long)]): Array[(Int, Int, Double, Double, Double, Double, Double, Double, Double)] = {

    val rankArr = Array(60, 130, 200)
    val iterationArr = Array(10, 15)
    val lambdaArr = Array(0.1, 1)
    val alphaArr = Array(0.1, 1, 10)

    val results = ArrayBuffer[(Int, Int, Double, Double, Double, Double, Double, Double, Double)]()
    for (rank <- rankArr; iteration <- iterationArr; lambda <- lambdaArr; alpha <- alphaArr)
    //   val result = yield {
    {
      println("parameters: ", rank, iteration, lambda, alpha)
      val model = ALS.trainImplicit(trainingRdd, rank, iteration, lambda, alpha)
      val f1score = getF1Score2(model, testingRdd)
      val rmse = getRMSE(model, testingRdd)
      val auc = 0.0
      //        val auc = getAUC(spark, model, testingRdd, stringLongVideoRdd)
      //      println("results   : ", LocalDateTime.now, rmse, f1score, auc)
      results.append((rank, iteration, lambda, alpha, rmse, f1score._1, f1score._2, f1score._3, auc))
    }

    results.toArray
  }


  def getRMSE(model: MatrixFactorizationModel, testingRdd: RDD[Rating]): Double = {
    val userProduct = testingRdd.map(row => (row.user, row.product))
    val actualScore = testingRdd.map(row => ((row.user, row.product), row.rating))

    val predictRating = model.predict(userProduct)
    val predictScore = predictRating.map(row => ((row.user, row.product), row.rating))

    sqrt(
      actualScore.join(predictScore).map {
        case ((uid, pid), (real, pred)) =>
          val err = real - pred
          err * err
      }.mean()
    )
  }

  /*def getAUC(spark: SparkSession, model: MatrixFactorizationModel, testingRdd: RDD[Rating], stringLongVideoRdd: RDD[(String, Long)]): Double = {
    import spark.implicits._
    val userProduct = testingRdd.map(row => (row.user, row.product))

    val positivePredictions = model.predict(userProduct)
      .map(row => (row.user, row.product, row.rating))
      .toDF("user", "product", "positivePrediction")

    val negativeData = testingRdd.map(row => (row.user, row.product))
      .toDF("user", "product")
      .as[(Int, Int)]
      .groupByKey { case (user, _) => user }
      .flatMapGroups {
        case (user, products) =>
          val random = new Random()
          val posItemSet = products.map { case (_, product) => product }.toSet
          val negative = new ArrayBuffer[Int]()
          val allProducts = stringLongVideoRdd.map(_._2.toInt).collect()

          var i = 0
          while (i < allProducts.length && negative.size < posItemSet.size) {
            val prodId = allProducts(random.nextInt(allProducts.length))
            if (!posItemSet.contains(prodId)) {
              negative.append(prodId)
            }
            i += 1
          }
          negative.map(prodId => (user, prodId))
      }.toDF("user", "product")
      .rdd
      .map(row => (row.getInt(0), row.getInt(1)))

    val negativePredictions = model.predict(negativeData)
      .map(row => (row.user, row.product, row.rating))
      .toDF("user", "product", "negativePrediction")

    val joinedPredictions = positivePredictions.join(negativePredictions, "user")
      .select("user", "positivePrediction", "negativePrediction")
      .cache()

    val allCounts = joinedPredictions.groupBy("user")
      .agg(count(lit("1")).as("total"))
      .select("user", "total")

    val correctCounts = joinedPredictions.filter($"positivePredictions" > $"negativePredictions")
      .groupBy("user")
      .agg(count("user").as("correct"))
      .select("user", "correct")

    val meanAUC = allCounts.join(correctCounts, Seq("user"), "left_outer")
      .select($"user", (coalesce($"correct", lit(0)) / $"total").as("auc"))
      .agg(mean("auc"))
      .as[Double]
      .first()

    meanAUC
  }*/

  def getF1Score(model: MatrixFactorizationModel, testingRdd: RDD[Rating]): (Double, Double, Double) = {
    import scala.util.control.Breaks._
    val userArray = testingRdd.map(row => row.user).distinct().collect()
    val recLength = 50

    var sumPrecision = 0.0
    var sumRecall = 0.0
    var sumF1Score = 0.0
    var cnt = 0

    println("start looping users, time: ", LocalDateTime.now)
    val userProductsRdd = testingRdd.map(row => (row.user, row.product))
      .distinct()
      .groupByKey()
      .map(x => (x._1, x._2.toArray))

    userProductsRdd.take(20).foreach(println)

    val predictUserProductsRdd = model.recommendProductsForUsers(recLength)

    predictUserProductsRdd.take(20).foreach(println)

    val userF1scoreRdd = userProductsRdd.join(predictUserProductsRdd)
      .map(x => {
        val actualProductsArr = x._2._1
        val predicProductsArr = x._2._2
          .map(_.product)

        var precision = 0.0
        var recall = 0.0
        var f1score = 0.0
        if (actualProductsArr.length != 0.0) {
          val hits = actualProductsArr.intersect(predicProductsArr).length.toDouble

          precision = (hits / recLength.toDouble).formatted("%.5f").toDouble
          recall = (hits / actualProductsArr.length.toDouble).formatted("%.5f").toDouble
          if (recall != 0.0) {
            f1score = (2 * precision * recall) / (precision + recall)
          }
        }

        println(LocalDateTime.now, " precision: " + precision, "recall: " + recall, "f1score: " + f1score)
        (x._1, f1score)
      })

    val value = userF1scoreRdd.map(_._2).mean()
    println(LocalDateTime.now, " mean f1score: " + value)

    for (user <- userArray) {
      //      println(LocalDateTime.now)
      var predictProductsArr: Array[Int] = null
      try {
        predictProductsArr = model.recommendProducts(user, recLength)
          .map(_.product)
      } catch {
        case e: NoSuchElementException => {}
      }

      val actualUserProductRdd = testingRdd.filter(_.user.equals(user))
        .map(_.product)
        .coalesce(1)
      val actualLength = actualUserProductRdd.count.toDouble

      /*      breakable {
              if (actualLength == 0.0 || predictProductsArr == null) {
                println("current_user: " + user, "actualLength: " + actualLength)
                break
              }
            }*/

      var precision = 0.0
      var recall = 0.0
      //      println("actualLength="+ actualLength, "predictArr: " + predictProductsArr)
      if (actualLength != 0.0) {
        if (predictProductsArr != null) {
          val hits = actualUserProductRdd.filter(x => predictProductsArr.contains(x))
            .count
            .toDouble
          precision = (hits / recLength.toDouble).formatted("%.5f").toDouble
          recall = (hits / actualLength).formatted("%.5f").toDouble
        }
      }

      if (recall != 0.0) {
        sumPrecision += precision
        sumRecall += recall
        sumF1Score += (2 * precision * recall) / (precision + recall)
      }
      cnt += 1

      //      println(LocalDateTime.now)
      /*if (cnt % 2000 == 0) {
        println(LocalDateTime.now, "cnt: " + cnt, "precision: " + (sumPrecision / cnt), "recall: " + (sumRecall / cnt), "f1score: " + (sumF1Score / cnt))
      }*/
    }

    //    println(LocalDateTime.now, "precision: " + (sumPrecision / cnt), "recall: " + (sumRecall / cnt), "f1score: " + (sumF1Score / cnt))
    ((sumPrecision / cnt).formatted("%.5f").toDouble, (sumRecall / cnt).formatted("%.5f").toDouble, (sumF1Score / cnt).formatted("%.5f").toDouble)
  }

  def getF1Score2(model: MatrixFactorizationModel, testingRdd: RDD[Rating]): (Double, Double, Double) = {

    val recLength = 50

    println("start time: " + LocalDateTime.now)
    val userProductsRdd = testingRdd.map(row => (row.user, row.product))
      .distinct()
      .groupByKey()
      .map(x => (x._1, x._2.toArray))

    val predictUserProductsRdd = model.recommendProductsForUsers(recLength)

    val userF1scoreRdd = userProductsRdd.join(predictUserProductsRdd)
      .map(x => {
        val actualProductsArr = x._2._1
        val predicProductsArr = x._2._2
          .map(_.product)

        var precision = 0.0
        var recall = 0.0
        var f1score = 0.0
        if (actualProductsArr.length != 0.0) {
          val hits = actualProductsArr.intersect(predicProductsArr).length.toDouble

          precision = (hits / recLength.toDouble).formatted("%.5f").toDouble
          recall = (hits / actualProductsArr.length.toDouble).formatted("%.5f").toDouble
          if (recall != 0.0) {
            f1score = (2 * precision * recall) / (precision + recall)
          }
        }

        (x._1, precision, recall, f1score)
      }).cache()

    var count = 0L
    try {
      count = userF1scoreRdd.count
    } catch {
      case ex: ArrayIndexOutOfBoundsException => println("交集长度为0啊")
    }

    var precision = 0.0
    var recall = 0.0
    var f1score = 0.0
    if (count > 0) {
      precision = userF1scoreRdd.map(_._2).mean()
      recall = userF1scoreRdd.map(_._3).mean()
      f1score = userF1scoreRdd.map(_._4).mean()
      userF1scoreRdd.persist()

      println("End   time: " + LocalDateTime.now)
      println("precision: " + precision, "recall: " + recall, "f1score: " + f1score)
    }
    (precision.formatted("%.5f").toDouble, recall.formatted("%.5f").toDouble, f1score.formatted("%.5f").toDouble)
  }
}
