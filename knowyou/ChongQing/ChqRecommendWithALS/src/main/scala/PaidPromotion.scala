import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.storage.StorageLevel

object PaidPromotion {

  var AUTOMATIC_DATE: Boolean = _

  var BEHAVIOR_SQL: String = _
  var ORDER_SQL: String = _
  var INSERT_TEMPORARY_SQL: String = _
  var INSERT_RESULT_SQL: String = _

  var ALS_RANK: Int = _
  var ALS_NUM_ITERATIONS: Int = _
  var ALS_LAMBDA: Double = _
  var ALS_ALPHA: Double = _

  val SPECIFY_PRODUCTION_NAME = "爱家影视月包（首月优惠）"

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")

    val conf = new SparkConf()
      .setAppName("PaidPromotion")
      //      .setMaster("local[*]")
      //      .set("yarn.resourcemanager.hostname", "hdp2")
      //      .set("spark.driver.host", "192.168.2.180")
      //      .set("spark.executor.instance", "3")
      //      .set("spark.executor.memory", "5g")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //      .set("hive.metastore.uris", "thrift://hdp2:9083")
    //      .setJars(List("D:\\a-sk\\programCode\\ChongQing\\ChqRecommendWithALS\\target\\ChqRecommendWithALS-1.0-SNAPSHOT-shaded.jar"))

    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    initConfig()

    start(spark)

    spark.stop()
  }


  def initConfig(): Unit = {
    System.out.println("初始化配置=============================================")

    val props = new Properties()
    props.load(Thread.currentThread().getContextClassLoader.getResourceAsStream("config.properties"))

    // 收视行为数据
    BEHAVIOR_SQL = props.getProperty("behaviorSql")
    // 订购行为数据
    ORDER_SQL = props.getProperty("orderDatSql")
    // 插入临时表
    INSERT_TEMPORARY_SQL = props.getProperty("insTemporarySQL")
    //插入结果表
    INSERT_RESULT_SQL = props.getProperty("insertResultSQL")

    // ALS.train所用参数配置
    ALS_RANK = props.getProperty("rank").toInt
    ALS_NUM_ITERATIONS = props.getProperty("numIterations").toInt
    ALS_LAMBDA = props.getProperty("lambda").toDouble
    ALS_ALPHA = props.getProperty("alpha").toDouble

  }

  def start(spark: SparkSession): Unit = {

    val startTime = System.currentTimeMillis()

    // Hive中查数据
    val orderRdd = spark.sql(ORDER_SQL).rdd
    val behaviorRdd = spark.sql(BEHAVIOR_SQL).rdd

    System.out.println(ORDER_SQL)
    System.out.println(BEHAVIOR_SQL)

    System.out.println("订购总数: " + orderRdd.count + "=============================================")
    System.out.println("收视总数: " + behaviorRdd.count + "=============================================")

    val queryTime = System.currentTimeMillis()
    System.out.println("数据查询时间 : " + (queryTime - startTime) / 1000 + "s=============================================")

    // rdd转换
    transformation(spark, behaviorRdd, orderRdd)

  }

  def transformation(spark: SparkSession, behaviorRdd: RDD[Row], orderRdd: RDD[Row]): Unit = {
    val startTime = System.currentTimeMillis()

    // 订购行为(device_id, production_name)
    val deviceProductionRdd = orderRdd.map(x => {
      if (2 == x.length) (x.getString(0), x.getString(1))
      else null
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    // 产品包及其id(production_name, index)
    val productionWithIndexRdd = deviceProductionRdd.map(_._2)
      .distinct(10)
      .zipWithIndex()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val maxProductionIndex = productionWithIndexRdd
      .map(x => (1, x._2))
      .reduceByKey(Math.max(_, _))
      .map(_._2)
      .take(1)
      .head

    System.out.println("产品包总数： " + (maxProductionIndex + 1) + "=============================================")

    // 收视行为(device_id, video_id)
    val deviceVideoRdd = behaviorRdd.map(x => {
      if (2 == x.length) (x.getString(0), x.getString(1))
      else null
    }).persist(StorageLevel.MEMORY_AND_DISK_SER)

    // 节目及其id(video_id, index + 1 + maxProductionIndex) 节目编号在矩阵中位于订购产品包之后，所以映射的索引初始值为产品包最大值+1
    val videoWithIndexRdd = deviceVideoRdd.map(_._2)
      .distinct(200)
      .zipWithIndex()
      .map(x => (x._1, x._2 + maxProductionIndex + 1))

    // 所有用户及其id(device_id, index)
    val deviceWithIndexRdd = deviceVideoRdd.map(_._1)
      .union(deviceProductionRdd.map(_._1))
      .distinct(100)
      .zipWithIndex()
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    // 构造用户索引与产品包索引rdd (device index, prod index)
    val deviceAndProductionIndexRdd = deviceProductionRdd
      .leftOuterJoin(deviceWithIndexRdd)
      // (device_id, (production_name, device index))
      .map(x => (x._2._1, x._2._2))
      .leftOuterJoin(productionWithIndexRdd)
      .map(x => (x._2._1.getOrElse(0), x._2._2.getOrElse(0)))
      .map(x => (x._1.toString.toInt, x._2.toString.toInt))

    // 构造用户索引与收视行为索引rdd (device index, video index)
    val deviceAndVideoIndexRdd = deviceVideoRdd
      .leftOuterJoin(deviceWithIndexRdd)
      // (device_id, (video_id, device index))
      .map(x => (x._2._1, x._2._2))
      .leftOuterJoin(videoWithIndexRdd)
      .map(x => (x._2._1.getOrElse(0), x._2._2.getOrElse(0)))
      .map(x => (x._1.toString.toInt, x._2.toString.toInt))

    // 用于训练的rdd，所有用户+所有产品和收视
    val ratingRdd = deviceAndProductionIndexRdd
      .union(deviceAndVideoIndexRdd)
      .map(x => Rating(x._1, x._2, 1.0))

    deviceVideoRdd.unpersist()
    deviceProductionRdd.unpersist()

    val transformationTime = System.currentTimeMillis()
    System.out.println("Rdd转换时间 : " + (transformationTime - startTime) / 1000 + "s=============================================")

    // ALS训练预测所有未订购用户与所有产品
    //    trainAndPredict(spark, deviceAndProductionIndexRdd, deviceAndVideoIndexRdd, ratingRdd, deviceWithIndexRdd, productionWithIndexRdd)

    // 爱家影视月包（首月优惠） 推荐
    trainAndPredict1YPackage(spark, deviceAndProductionIndexRdd, deviceAndVideoIndexRdd, ratingRdd, deviceWithIndexRdd, productionWithIndexRdd)

  }

  def trainAndPredict(spark: SparkSession,
                      deviceAndProductionIndexRdd: RDD[(Int, Int)],
                      deviceAndVideoIndexRdd: RDD[(Int, Int)],
                      ratingRdd: RDD[Rating],
                      deviceWithIndexRdd: RDD[(String, Long)],
                      productionWithIndexRdd: RDD[(String, Long)]): Unit = {
    val startTime = System.currentTimeMillis()

    // 采用隐式反馈模型
    val model = ALS.trainImplicit(ratingRdd, ALS_RANK, ALS_NUM_ITERATIONS, ALS_LAMBDA, ALS_ALPHA)

    // 用于预测的产品包集
    val predictProductionIndexRdd = productionWithIndexRdd
      .map(_._2.toInt)

    // 用于预测的设备（用户）集 = 所有收视用户 - 订购用户
    val predictDeviceIndexRdd = deviceAndVideoIndexRdd
      .map(_._1)
      .subtract(deviceAndProductionIndexRdd.map(_._1))


    // 预测数据做笛卡尔积
    val predictDeviceAndProductionRdd = predictDeviceIndexRdd
      .cartesian(predictProductionIndexRdd)
      // TODO
      .coalesce(1)

    val predictResultWithIndexRdd = model.predict(predictDeviceAndProductionRdd)


    // index与名称映射转换
    val predictResultWithIdRdd = predictResultWithIndexRdd
      .map(x => (x.user, (x.product, x.rating)))
      .join(deviceWithIndexRdd.map(x => (x._2.toInt, x._1)))
      .map(x => (x._2._1._1, (x._2._2, x._2._1._2)))
      .join(productionWithIndexRdd.map(x => (x._2.toInt, x._1)))
      .map(x => (x._2._1._1, x._2._2, BigDecimal.valueOf(x._2._1._2).toString()))


    val trainingTime = System.currentTimeMillis()
    System.out.println("ALS模型训练及预测时间 : " + (trainingTime - startTime) / 1000 + "s=============================================")

    // 插入结果表
    insertData(spark, predictResultWithIdRdd)
  }

  def trainAndPredict1YPackage(spark: SparkSession,
                               deviceAndProductionIndexRdd: RDD[(Int, Int)],
                               deviceAndVideoIndexRdd: RDD[(Int, Int)],
                               ratingRdd: RDD[Rating],
                               deviceWithIndexRdd: RDD[(String, Long)],
                               productionWithIndexRdd: RDD[(String, Long)]): Unit = {
    val startTime = System.currentTimeMillis()

    // 采用隐式反馈模型
    val model = ALS.trainImplicit(ratingRdd, ALS_RANK, ALS_NUM_ITERATIONS, ALS_LAMBDA, ALS_ALPHA)

    // 爱家影视月包（首月优惠） 找其id
    val predict1YPackageIndex = productionWithIndexRdd
      .filter(_._1.equals("爱家影视月包（首月优惠）"))
      .map(_._2.toInt)
      .take(1)
      .head

    val num = deviceWithIndexRdd.count()

    // 推荐用户
    val recommend1YPackageResult = model.recommendUsers(predict1YPackageIndex, num.toInt)

    System.out.println(SPECIFY_PRODUCTION_NAME + "推荐用户个数：" + recommend1YPackageResult.length + "=============================================")

    val resultRdd = spark.sparkContext.parallelize(recommend1YPackageResult)

    // 从推荐用户中去掉订购用户，不在此处过滤，结果在excel中筛选
    //    val withoutOrderDeviceRdd = resultRdd
    //      .map(x => x.user)
    //      .subtract(deviceAndProductionIndexRdd.map(_._1))

    //    System.out.println("no订购用户个数：" + withoutOrderDeviceRdd.count() + "=============================================")

    // index与名称映射转换
    val predictResultWithIdRdd = resultRdd
      .map(x => (x.user, (x.product, x.rating)))
      //      .join(withoutOrderDeviceRdd.map((_, "")))
      //      .map(x => (x._1, x._2._1))
      .join(deviceWithIndexRdd.map(x => (x._2.toInt, x._1)))
      .map(x => (x._2._2, "爱家影视月包（首月优惠）", BigDecimal.valueOf(x._2._1._2).toString()))

    //    System.out.println("最终写入hive结果数：" + predictResultWithIdRdd.count() + "=============================================")

    deviceWithIndexRdd.unpersist()
    productionWithIndexRdd.unpersist()

    val trainingTime = System.currentTimeMillis()
    System.out.println("ALS模型训练及预测时间 : " + (trainingTime - startTime) / 1000 + "s=============================================")

    // 插入结果表
    insertData(spark, predictResultWithIdRdd)
  }

  def insertData(spark: SparkSession, predictResultWithIdRdd: RDD[(String, String, String)]): Unit = {
    val startTime = System.currentTimeMillis()

    val schemaString = "device_id prod_id rating"

    // 定义schema
    val fields = schemaString.split(" ")
      .map(fieldName => fieldName match {
        //        case "rating" => StructField(fieldName, DoubleType, nullable = true)
        case _ => StructField(fieldName, StringType, nullable = true)
      })
    val schema = StructType(fields)

    // 创建Rdd[Row]
    val insertResultRowRdd = predictResultWithIdRdd.map(x => Row(x._1, x._2, x._3))

    val htvPayPlusDF = spark.createDataFrame(insertResultRowRdd, schema)

    System.out.println("结果数量：" + htvPayPlusDF.count + "=============================================")

    htvPayPlusDF.createOrReplaceTempView("final_pay")

    System.out.println(INSERT_TEMPORARY_SQL)

    spark.sql(INSERT_TEMPORARY_SQL)

    val insertTime = System.currentTimeMillis()
    System.out.println("结果数据插入时间 : " + (insertTime - startTime) / 1000 + "s=============================================")
  }
}
