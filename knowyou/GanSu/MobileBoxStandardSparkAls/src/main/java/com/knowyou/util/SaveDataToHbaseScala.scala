package com.knowyou.util

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}


/**
 * Created by wengxw on 2019/3/29
 */
object SaveDataToHbaseScala {
  def saveAsNewAPIHadoopDataSet(sparkSession: SparkSession, dataset: Dataset[Row], tableName: String, columnName: String): Unit = {
    //创建HBase连接
    val hbaseConf = HBaseConfiguration.create()
    val config = Config.getInstance()
    val zk_quorum = config.getProperty(UtilConstants.Public.HBASE_ZOOKEEPER_QUORUM)
    val zkport = config.getProperty(UtilConstants.Public.HBASE_ZOOKEEPER_CLIENTPORT)
    val znode = config.getProperty(UtilConstants.Public.HBASE_ZNODE_PARENT)
    //        hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
    hbaseConf.set("spark.executor.memory", "3000m")
    //hbaseConf.set("hbase.zookeeper.quorum", "172.16.1.220:2181,172.16.1.221:2181,172.16.1.222:2181")
    //        hbaseConf.set("hbase.zookeeper.quorum", "hadoop1:2181,hadoop2:2181,hadoop3:2181,kafka1:2181,kafka2:2181,kafka3:2181")
    hbaseConf.set(UtilConstants.Public.HBASE_ZOOKEEPER_QUORUM,zk_quorum)
    hbaseConf.set(UtilConstants.Public.HBASE_ZOOKEEPER_CLIENTPORT, zkport)
    hbaseConf.set(UtilConstants.Public.HBASE_ZNODE_PARENT,znode)
    //        hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
    //        val connection = ConnectionFactory.createConnection(hbaseConf)
    //表名
    val columnFamily = "info"
    //字段名
    val field = "rowkey"
    //创建配置项
    val jobConf = new JobConf(hbaseConf)
    //设置表名
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    //过滤掉空值
    val dsf = dataset.filter(row => (row.getAs(field) != null) && (row.getAs(columnName) != null))
    //解析数据
    val data = dsf.rdd.mapPartitions{iter =>
      iter.map{row =>
        val rowkey = row.getAs(field).toString
        val columnValue = row.getAs(columnName).toString
        val put = new Put(rowkey.getBytes())
        //            put.addColumn(columnFamily.getBytes(), labelname.getBytes(), labelValue.getBytes())
        put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(columnValue))
        (new ImmutableBytesWritable(), put)

      }
    }
    //将数据写入hbase中
    data.saveAsHadoopDataset(jobConf)
  }

  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession
      .builder
      .config("spark.sql.shuffle.partitions", 100)
      .config("spark.hadoop.validateOutputSpecs", "false")
//                        .master("local[*]")
      .appName("saveAsHadoopDataset")
      .enableHiveSupport
      .getOrCreate

    sparkSession.sparkContext.setLogLevel("DEBUG")
    import sparkSession.implicits._

    val frame = sparkSession.sql("select device_id as rowkey,video_id from knowyou_jituan_dmt.ads_rec_userdetail_wtr limit 5")
    frame.show(false)
    try {
      saveAsNewAPIHadoopDataSet(sparkSession, frame, "test", "video_id")
    } catch {
      case e:Exception => {
        println("test_sk_2 occur exception")
        e.printStackTrace()
      }
    }


    sparkSession.stop()

  }

}
