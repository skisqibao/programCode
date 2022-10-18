import org.slf4j.{Logger, LoggerFactory}

object HelloTest {
  def main(args: Array[String]): Unit = {
    val LOG = LoggerFactory.getLogger(HelloTest.getClass)
    LOG.info("Hello")
    LOG.error("BUG")
    println("world")
    val t1: Any = 1234444l
    val r = 1.1111111111111E-7
    //    println(t1.toInt)
    println( BigDecimal.valueOf(r).toString())
//    println(t1.asInstanceOf[Int])
    val ite  =  Iterable[String] ("Aaa","Bbb","Ccc")
    println(ite.toString().substring(5, ite.toString.length-1).replaceAll("\\s",""))
  }
}
