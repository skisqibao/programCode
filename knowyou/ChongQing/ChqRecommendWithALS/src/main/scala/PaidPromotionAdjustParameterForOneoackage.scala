import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.mllib.recommendation.{ALS, Rating}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory


object PaidPromotionAdjustParameterForOneoackage {
  val logger = LoggerFactory.getLogger(PaidPromotionAdjustParameter.getClass)

  /**
   * 本地测试，用于调参
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    //    System.setProperty("hadoop.home.dir", HADOOP_HOME_DIR)

    val conf = new SparkConf()
      .setAppName("PaidPromotionAdjustParameter")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 设置可直接覆盖文件路径
      .set("spark.hadoop.validateOutputSpecs", "false")

    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()


//    handleVideoData(spark)
//    handleOrderData(spark)
//    transformationRdd(spark)
    trainAndPredict(spark)

    spark.stop()
  }

  /**
   * 收视行为，无需分割全部未训练集，可根据数据量选择取样
   *
   * @param spark
   */
  def handleVideoData(spark: SparkSession): Unit = {

    // 去重去空观看行为大于30的收视数据集
    val behaviorLineRdd = spark.sparkContext.textFile("/sk/chongqing/data/behavior_data.bcp")

    logger.info("收视总数" + behaviorLineRdd.count)

    // 收视行为(device_id, video_id)
    val deviceVideoRdd = behaviorLineRdd.map(x => {
      (x.split("\t")(0), x.split("\t")(1))
    })

    // 统计用户活跃度，即每个用户收视行为次数
    val deviceActiveRdd = deviceVideoRdd.map(x => (x._1, 1)).reduceByKey(_ + _)
    val deviceVideoNumber = deviceActiveRdd.count
    logger.info("用户总数" + deviceVideoNumber)

    // 去掉1/6最活跃用户和1/3最不活跃用户
    val littleDeviceSet = deviceActiveRdd.map(_.swap)
      .sortByKey(false)
      .zipWithIndex()
      .filter(x => x._2 >= deviceVideoNumber / 6 && x._2 <= deviceVideoNumber * 2 / 3)
      .map(x => x._1._2)
      .takeSample(false, 40000)

    //    logger.info("小数据集个数：" + littleDeviceSet.length)

    val sampleDeviceRdd = deviceVideoRdd
      .filter(x => littleDeviceSet.contains(x._1))
      .map(x => x._1 + "," + x._2)

    //    logger.info("抽样总数：" + sampleDeviceRdd.count)

    sampleDeviceRdd.coalesce(12).saveAsTextFile("/sk/chongqing/handle_data/device_video_data/")
  }

  /**
   * 处理订购数据，数据二八分，验证集超过两万时，多于两万的放回训练集
   *
   * @param spark
   */
  def handleOrderData(spark: SparkSession): Unit = {

    // 去重去空过滤后的订购数据集
    val orderLineRdd = spark.sparkContext.textFile("/sk/chongqing/data/order_data.bcp")

//    logger.info("订购总数" + orderLineRdd.count)

    // lineRdd ==> kvRdd
    // 订购行为(device_id, production_name)
    val deviceProductionRdd = orderLineRdd.map(x => {
      (x.split("\t")(0), x.split("\t")(1))
    })

    val count = deviceProductionRdd.count
    logger.info("订购总数：" + count)

    var splitRdd: Array[RDD[(String, String)]] = Array()
    if (count > 100000) {
//      val ratio = 60000.0 / count
      val ratio = 0.5
      splitRdd = deviceProductionRdd.randomSplit(Array(1 - ratio, ratio))
    } else {
      splitRdd = deviceProductionRdd.randomSplit(Array(0.7, 0.3))
    }

    val trainSetRdd = splitRdd(0)
    val validationSetRdd = splitRdd(1)
    logger.info("初次切分训练集数量：" + trainSetRdd.count)
    logger.info("初次切分验证集数量：" + validationSetRdd.count)

    /**
     * 遍历检查验证集中是否存在训练集中没有的用户id或产品包名，如果存在，则把该条记录从验证集移回至训练集中。
     */
    // 训练集与验证集的device_id交集
    val trainDeviceRdd = trainSetRdd.map(x => x._1).distinct()
    val validationDeviceRdd = validationSetRdd.map(x => x._1).distinct()
    val intersectionDeviceRdd = trainDeviceRdd.intersection(validationDeviceRdd)

    // 训练集与验证集的production交集
    val trainProductionRdd = trainSetRdd.map(x => x._2).distinct()
    val validationProductionRdd = validationSetRdd.map(x => x._2).distinct()
    val intersectionProductionRdd = trainProductionRdd.intersection(validationProductionRdd)

    logger.info("-----------------------------------");
    logger.info("训练集用户数 = " + trainDeviceRdd.count())
    logger.info("验证集用户数 = " + validationDeviceRdd.count())
    logger.info("共 同 用户数 = " + intersectionDeviceRdd.count())
    logger.info("训练集节目数 = " + trainProductionRdd.count())
    logger.info("验证集节目数 = " + validationProductionRdd.count())
    logger.info("共 同 节目数 = " + intersectionProductionRdd.count())

    // 得到有效用户集和产品集
    val validDeviceArray = intersectionDeviceRdd.collect()
    val validProductionArray = intersectionProductionRdd.collect()

    // 得出有效的验证集记录，筛选出用户和产品都存在于训练集中的记录
    val finalValidationSetRdd = validationSetRdd.filter(x => validDeviceArray.contains(x._1) && validProductionArray.contains(x._2))

    // 无效的验证集记录转移至训练集
    val finalTrainSetRdd = validationSetRdd.filter(x => !(validDeviceArray.contains(x._1) && validProductionArray.contains(x._2)))
      .union(trainSetRdd)

    logger.info("最终训练集数量：" + finalTrainSetRdd.count())
    logger.info("最终验证集数量：" + finalValidationSetRdd.count)

    // 求最终训练集和验证集的用户id交集，正常情况交集数量应该和验证集相等
    val finalTrainDeviceRdd = finalTrainSetRdd.map(x => x._1).distinct()
    val finalValidationDeviceRdd = finalValidationSetRdd.map(x => x._1).distinct()
    val finalIntersectionDeviceRdd = finalTrainDeviceRdd.intersection(finalValidationDeviceRdd)

    val finalTrainProductionRdd = finalTrainSetRdd.map(x => x._2).distinct()
    val finalValidationProductionRdd = finalValidationSetRdd.map(x => x._2).distinct()
    val finalIntersectionProductionRdd = finalTrainProductionRdd.intersection(finalValidationProductionRdd)

    logger.info("-----------------------------------");
    logger.info("最终训练集用户数 = " + finalTrainDeviceRdd.count())
    logger.info("最终验证集用户数 = " + finalValidationDeviceRdd.count())
    logger.info("最终共 同 用户数 = " + finalIntersectionDeviceRdd.count())
    logger.info("最终训练集节目数 = " + finalTrainProductionRdd.count())
    logger.info("最终验证集节目数 = " + finalValidationProductionRdd.count())
    logger.info("最终共 同 节目数 = " + finalIntersectionProductionRdd.count())


    finalTrainSetRdd.map(x => x._1 + "," + x._2)
      .coalesce(1)
      .saveAsTextFile("/sk/chongqing/handle_data/set-train-device-production-data/")
    finalValidationSetRdd.map(x => x._1 + "," + x._2)
      .coalesce(1)
      .saveAsTextFile("/sk/chongqing/handle_data/set-validation-device-production-data/")

    // 统计出验证集的用户列表，生成并保存每个用户所订购的产品包列表
    val validationDeviceListRdd = finalValidationSetRdd.groupByKey()
      .map(x => {
        val buffer = x._2.toString()
        x._1 + "#" + buffer.substring(14, buffer.length - 1).replaceAll("\\s", "")
      })

    logger.info("验证集用户订购列表-----------------------------------")
    logger.info("验证集用户列表数量：" + validationDeviceListRdd.count)
    validationDeviceListRdd.take(20).foreach(println)

    // 统计出验证集的产品包列表，生成并保存每个产品包的订购用户列表
    val validationProductionListRdd = finalValidationSetRdd.map(_.swap)
      .groupByKey()
      .map(x => {
        val buffer = x._2.toString()
        x._1 + "#" + buffer.substring(14, buffer.length - 1).replaceAll("\\s", "")
      })

    logger.info("验证集产品包列表-----------------------------------")
    logger.info("验证集产品包列表数量：" + validationProductionListRdd.count)
    validationProductionListRdd.take(20).foreach(println)

    // 统计出训练集的用户列表，生成并保存每个用户所订购的产品包列表
    val trainDeviceListRdd = finalTrainSetRdd.groupByKey()
      .map(x => {
        val buffer = x._2.toString()
        x._1 + "#" + buffer.substring(14, buffer.length - 1).replaceAll("\\s", "")
      })

    logger.info("训练集用户订购列表-----------------------------------")
    logger.info("训练集用户列表数量：" + trainDeviceListRdd.count)
    trainDeviceListRdd.take(20).foreach(println)

    // 统计出训练集产品包列表，生成并保存每个产品包的订购用户列表
    val trainProductionListRdd = finalTrainSetRdd.map(_.swap)
      .groupByKey()
      .map(x => {
        val buffer = x._2.toString()
        x._1 + "#" + buffer.substring(14, buffer.length - 1).replaceAll("\\s", "")
      })

    logger.info("训练集产品包列表-----------------------------------")
    logger.info("训练集产品包列表数量：" + trainProductionListRdd.count)
    trainProductionListRdd.take(20).foreach(println)

    validationDeviceListRdd.coalesce(1).saveAsTextFile("/sk/chongqing/list/validation-device")
    validationProductionListRdd.coalesce(1).saveAsTextFile("/sk/chongqing/list/validation-production")
    trainDeviceListRdd.coalesce(1).saveAsTextFile("/sk/chongqing/list/train-device")
    trainProductionListRdd.coalesce(1).saveAsTextFile("/sk/chongqing/list/train-production")
  }

  /**
   * 数据格式处理，转化为模型需要的数据结构
   *
   * @param spark
   */
  def transformationRdd(spark: SparkSession): Unit = {

    val startTime = System.currentTimeMillis()
    val orderRdd = spark.sparkContext.textFile("/sk/chongqing/handle_data/set-train-device-production-data/")

    // 订购行为(device_id, production_name)
    val deviceProductionRdd = orderRdd.map(x => {
      (x.split(",")(0), x.split(",")(1))
    })

    logger.info("订购行为训练集个数：" + deviceProductionRdd.count)

    // 产品包及其id(production_name, index)
    val productionWithIndexRdd = deviceProductionRdd.map(_._2)
      .distinct(3)
      .zipWithIndex()

    val maxProductionIndex = productionWithIndexRdd
      .map(x => (1, x._2))
      .reduceByKey(Math.max(_, _))
      .map(_._2)
      .take(1)
      .head

    logger.info("训练集产品包总数： " + maxProductionIndex)


    val behaviorRdd = spark.sparkContext.textFile("/sk/chongqing/handle_data/device_video_data/")

    // 收视行为(device_id, video_id)
    val deviceVideoRdd = behaviorRdd.map(x => {
      (x.split(",")(0), x.split(",")(1))
    })

    logger.info("收视行为总数：" + deviceVideoRdd.count)

    // 节目及其id(video_id, index + 1 + maxProductionIndex) 节目编号在矩阵中位于订购产品包之后，所以映射的索引初始值为产品包最大值+1
    val videoWithIndexRdd = deviceVideoRdd.map(_._2)
      .distinct(12)
      .zipWithIndex()
      .map(x => (x._1, x._2 + maxProductionIndex))

    // 所有用户及其id(device_id, index)
    val deviceWithIndexRdd = deviceVideoRdd.map(_._1)
      .union(deviceProductionRdd.map(_._1))
      .distinct(3)
      .zipWithIndex()

    logger.info("训练总用户数：" + deviceWithIndexRdd.count)

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
      .map(x => x._1 + "," + x._2)

    val transformationTime = System.currentTimeMillis()
    logger.info("Rdd转换时间 : " + (transformationTime - startTime) / 1000 + "s")

    ratingRdd.coalesce(10).saveAsTextFile("/sk/chongqing/model/input/rating/")
    productionWithIndexRdd.map(x => x._1 + "," + x._2).coalesce(1).saveAsTextFile("/sk/chongqing/model/input/productionWithIndex/")
    deviceWithIndexRdd.map(x => x._1 + "," + x._2).coalesce(1).saveAsTextFile("/sk/chongqing/model/input/deviceWithIndex/")

  }

  def trainAndPredict(spark: SparkSession): Unit = {
    val startTime = System.currentTimeMillis()

    val ratingRdd = spark.sparkContext.textFile("/sk/chongqing/model/input/rating/")
      .map(x => Rating(x.split(",")(0).toInt, x.split(",")(1).toInt, 1.0))

    val props = new Properties()
    props.load(Thread.currentThread().getContextClassLoader.getResourceAsStream("config.properties"))

    val ALS_RANK = props.getProperty("rank").toInt
    val ALS_NUM_ITERATIONS = props.getProperty("numIterations").toInt
    val ALS_LAMBDA = props.getProperty("lambda").toDouble
    val ALS_ALPHA = props.getProperty("alpha").toDouble


    // 采用隐式反馈模型
    val model = ALS.trainImplicit(ratingRdd, ALS_RANK, ALS_NUM_ITERATIONS, ALS_LAMBDA, ALS_ALPHA)

    val productionWithIndex = spark.sparkContext.textFile("/sk/chongqing/model/input/productionWithIndex/")
    val productionWithIndexRdd = productionWithIndex.map(x => (x.split(",")(0), x.split(",")(1).toInt))

    val SPECIFY_PRODUCTION_NAME = "爱家影视月包（首月优惠）"
    // 爱家影视月包（首月优惠） 找其id
    val predictSpecifyPackageIndex = productionWithIndexRdd
      .filter(_._1.equals(SPECIFY_PRODUCTION_NAME))
      .map(_._2.toInt)
      .take(1)
      .head

    logger.info("当前指定产品包为：" + SPECIFY_PRODUCTION_NAME + "; 产品包本次id为：" + predictSpecifyPackageIndex)

    // 对每个产品包推荐用户，取topN个, N取值借鉴验证集每个产品包订购用户的平均值 ValidationListSize
    val ratings = model.recommendUsers(predictSpecifyPackageIndex, 25000)
    val recommendUsersRdd = spark.sparkContext.parallelize(ratings)

    val deviceWithIndexRdd = spark.sparkContext.textFile("/sk/chongqing/model/input/deviceWithIndex/")
      .map(x => (x.split(",")(0), x.split(",")(1).toInt))

    // index与名称映射转换
    val predictResultWithIdRdd = recommendUsersRdd
      .map(x => (x.user, (x.product, x.rating)))
      .join(deviceWithIndexRdd.map(_.swap))
      .map(x => (x._2._1._1, (x._2._2, x._2._1._2)))
      .join(productionWithIndexRdd.map(_.swap))
      .map(x => (x._2._1._1, x._2._2, x._2._1._2))

    // 当前产品包下 预测订购用户列表
    val predictResultDeviceList = predictResultWithIdRdd.map(_._1).collect
    val predictLength = predictResultDeviceList.length

    logger.info("预测结果列表长度 ：{}", predictLength)
    predictResultDeviceList.take(10).foreach(println)

    val validationProductionRdd = spark.sparkContext.textFile("/sk/chongqing/list/validation-production/")
      .map(x => (x.split("#")(0), x.split("#")(1)))
    // 当前产品包下 实际订购用户列表
    val actualResultDeviceList = validationProductionRdd.filter(_._1.equals(SPECIFY_PRODUCTION_NAME))
      .flatMap(_._2.split(","))
    val actualLength = actualResultDeviceList.count

    logger.info("实际订购列表长度 ：{}", actualLength)
    actualResultDeviceList.take(10).foreach(println)

    // 命中用户数
    val hit = actualResultDeviceList.filter(x => predictResultDeviceList.contains(x)).count.toDouble

    // 当前用户f1-score
    val f1score = (2 * hit) / (predictLength + actualLength)

    logger.info("\n产品包Index: " + predictSpecifyPackageIndex + "\t产品包Name: " + SPECIFY_PRODUCTION_NAME + "\n"
      + "预测订购列表用户数: " + predictLength + "\t实际订购列表用户数: " + actualLength + "\n"
      + "预测命中数：" + hit + "\n"
      + "当前f1-score累加值: " + f1score + "\t当前包数：" + "1")

    logger.info("最终f1-score平均值\tALS_RANK\tALS_NUM_ITERATIONS\tALS_LAMBDA\tALS_ALPHA\n"
      + BigDecimal.valueOf(f1score) + "\t" + ALS_RANK + "\t" + ALS_NUM_ITERATIONS + "\t" + ALS_LAMBDA + "\t" + ALS_ALPHA)

    val trainingTime = System.currentTimeMillis()
    logger.info("ALS模型训练及预测时间 : " + (trainingTime - startTime) / 1000 + "s")

  }
}
