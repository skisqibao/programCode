package com.knowyou

import java.util.Properties

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.TableOutputFormat
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf


/**
 * Created by wengxw on 2019/3/29
 */
object SaveDataToHbaseScala {
    def saveAsNewAPIHadoopDataSet(sparkSession: SparkSession, dataset: Dataset[Row], tableName: String, columnName: String): Unit = {
        //创建HBase连接
        val configuration: Configuration = new Configuration()
        val hbaseConf = HBaseConfiguration.create(configuration)

        val props = new Properties
        //        props.load(ClassLoader.getSystemClassLoader().getResourceAsStream("hbase.properties"));
        props.load(Thread.currentThread
          .getContextClassLoader.getResourceAsStream
        ("config" +
          ".properties"))

//        hbaseConf.set("spark.executor.memory", "3000m")
//        hbaseConf.addResource(".\\main\\resources\\hbase-site.xml")
        hbaseConf.set("hbase.zookeeper.quorum", props.getProperty("zookeeper.quorum"))
        hbaseConf.set("hbase.zookeeper.property.clientPort", props.getProperty("zookeeper.property.clientPort"))
        hbaseConf.set("zookeeper.znode.parent", props.getProperty("zookeeper.znode.parent"))
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
        val data = dsf.rdd.coalesce(1).mapPartitions{iter =>
            iter.map{row =>
                val rowkey = row.getAs(field).toString
                val columnValue = row.getAs(columnName).toString
                val put = new Put(rowkey.getBytes())
//                println("------------------sucess")
                //            put.addColumn(columnFamily.getBytes(), labelname.getBytes(), labelValue.getBytes())
                put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(columnName), Bytes.toBytes(columnValue))
                (new ImmutableBytesWritable(), put)

            }
        }
        //将数据写入hbase中
        data.saveAsHadoopDataset(jobConf)
    }


}
