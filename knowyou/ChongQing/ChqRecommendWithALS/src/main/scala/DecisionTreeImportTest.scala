import java.io.{FileInputStream, FileNotFoundException, IOException}

import javax.xml.bind.JAXBException
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.dmg.pmml.PMML
import org.jpmml.evaluator.{Evaluator, ModelEvaluatorFactory}
import org.xml.sax.SAXException

import scala.collection.mutable

object DecisionTreeImportTest {

  var inputStream: FileInputStream = _
  var pmml: PMML = _

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "root")
    val conf = new SparkConf()
      .setAppName("DecisionTreeImportTest")
      .setMaster("local[*]")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // 设置可直接覆盖文件路径
      .set("spark.hadoop.validateOutputSpecs", "false")

    val spark = SparkSession.builder()
      .config(conf)
      .enableHiveSupport()
      .getOrCreate()

    val demo = DecisionTreeImportTest
    val model = demo.loadPmml()

    demo.predict(model, 1, 8, 99, 1)
    demo.predict(model, 111, 89, 9, 11)

    spark.stop()
  }

  def loadPmml(): Evaluator = {
    try {
      inputStream = new FileInputStream("D:\\a-sk\\programCode\\ml\\decision_tree\\iris.pmml")
    } catch {
      case ex: FileNotFoundException => {
        println("Missing file")
        ex.printStackTrace()
      }
      case ex: IOException => {}
        println("IO Exception")
        ex.printStackTrace()
    }

    if (inputStream == null) {
      return null
    }

    try {
      pmml = org.jpmml.model.PMMLUtil.unmarshal(inputStream)
    } catch {
      case ex: SAXException => {
        println("SAX Exception")
        ex.printStackTrace()
      }
      case ex: JAXBException => {
        println("JAXB Exception")
        ex.printStackTrace()
      }
    } finally {
      inputStream.close()
    }

    val modelEvaluatorFactory = ModelEvaluatorFactory.newInstance()
    val evaluator = modelEvaluatorFactory.newModelEvaluator(pmml)

    evaluator
  }

  def predict(evaluator: Evaluator, a: Int, b: Int, c: Int, d: Int): Int = {
    var data: Map[String, Int] = Map()
    data += ("x1" -> a)
    data += ("x2" -> b)
    data += ("x3" -> c)
    data += ("x4" -> d)

    val inputFields = evaluator.getInputFields

    0

  }
}
