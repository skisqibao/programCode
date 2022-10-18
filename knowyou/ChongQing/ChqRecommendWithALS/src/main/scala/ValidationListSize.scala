import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object ValidationListSize {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    //    System.setProperty("hadoop.home.dir", HADOOP_HOME_DIR)

    val conf = new SparkConf()
      .setAppName("ValidationListSize")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 设置可直接覆盖文件路径
      .set("spark.hadoop.validateOutputSpecs", "false")

    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val productionListRdd = spark.sparkContext.textFile("/sk/chongqing/list/validation-production/")

    val productionAndDeviceListRdd = productionListRdd.map(x => (x.split("#")(0), x.split("#")(1)))

    val productionNumber = productionAndDeviceListRdd.count
    val productionNumber1 = productionAndDeviceListRdd.map(_._1).distinct.count

    val sumDeviceNumber = productionAndDeviceListRdd.map(x => ("A", x._2.split(",").length))
      .reduceByKey(_ + _)
      .map(_._2)
      .take(1)
      .head

    val sumDeviceNumber1 = productionAndDeviceListRdd.flatMap(x => x._2.split(",")).count
    System.out.println(sumDeviceNumber1)
    System.out.println(productionNumber1)
    System.out.println("sumDeviceNumber\t+\tproductionNumber\t=? ")
    System.out.println(sumDeviceNumber + "\t+\t" + productionNumber + "\t=\t" + (sumDeviceNumber / productionNumber))
  }
}
