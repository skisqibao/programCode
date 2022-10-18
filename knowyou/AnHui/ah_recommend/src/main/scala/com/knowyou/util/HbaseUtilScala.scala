//package com.knowyou.util
//
//import org.apache.hadoop.hbase.HBaseConfiguration
//import org.apache.hadoop.hbase.client.Put
//import org.apache.hadoop.hbase.io.ImmutableBytesWritable
//import org.apache.hadoop.hbase.mapred.TableOutputFormat
//import org.apache.hadoop.hbase.mapreduce.TableInputFormat
//import org.apache.hadoop.hbase.util.Bytes
//import org.apache.hadoop.mapred.JobConf
//import org.apache.spark.sql.{Dataset, Row, SparkSession}
//
//object HbaseUtilScala {
//  def saveAsNewAPIHadoopDataSet(sparkSession: SparkSession, dataset: Dataset[Row], tableName: String, columnName: String): Unit = {
//    //创建HBase连接
//    val hbaseConf = HBaseConfiguration.create()
//    hbaseConf.set("hbase.zookeeper.property.clientPort", "2181")
//    hbaseConf.set("spark.executor.memory", "3000m")
//    //        hbaseConf.set("hbase.zookeeper.quorum", "192.168.1.11:2181,192.168.1.12:2181,192.168.1.13:2181,192.168.1.10:2181")
//    hbaseConf.set("hbase.zookeeper.quorum", "10.191.183.204,10.191.183.205,10.191.183.206,10.191.183.208")
//    hbaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
//    hbaseConf.set("hbase.defaults.for.version.skip", "true")
//    hbaseConf.set(TableInputFormat.INPUT_TABLE, tableName)
//    //        val connection = ConnectionFactory.createConnection(hbaseConf)
//    //表名
//    val columnFamily = "info"
//    //字段名
//    val field = "rowkey"
//    //创建配置项
//    val jobConf = new JobConf(hbaseConf)
//    //设置表名
//    jobConf.setOutputFormat(classOf[TableOutputFormat])
//    jobConf.set(TableOutputFormat.OUTPUT_TABLE, tableName)
//    //过滤掉空值
//    val dsf = dataset.filter(row => (row.getAs(field) != null) && (row.getAs(columnName) != null))
//    //解析数据
//    val data = dsf.rdd.map(row => {
//      val rowkey = row.getAs(field).toString
//      val columnValue = row.getAs(columnName).toString
//      val put = new Put(rowkey.getBytes())
//      //            put.addColumn(columnFamily.getBytes(), labelname.getBytes(), labelValue.getBytes())
//      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(columnValue))
//      (new ImmutableBytesWritable(), put)
//    })
//    //将数据写入hbase中
//    data.saveAsHadoopDataset(jobConf)
//  }
//}
