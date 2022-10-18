package com.knowyou.task;

import java.util.ArrayList;
import java.util.List;


import com.knowyou.util.Record;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TestWriteHive {

    public static void main(String[] args) {
//        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();
/*        SparkConf conf = new SparkConf()
                .setAppName("TestWriteHive")
                .setMaster("local[*]")
                .set("yarn.resourcemanager.hostname", "hdp2")
                .set("spark.driver.host", "192.168.2.180")
                .set("spark.executor.instance", "3")
                .set("spark.executor.memory", "5g")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("hive.metastore.uris", "thrift://172.16.1.220:9083")
                .set("hive.exec.scratchdir", "hdfs://172.16.1.222:8020/user/hive/tmp");*/

        SparkSession ss = SparkSession.builder()
                .appName("Java Spark Hive Example")
                .master("local[*]")
//                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()
                .getOrCreate();

//        ss.sql("desc knowyou_ott_ods.ods_uba_cn_videoplayinfo").show();

//        ss.sql("select deviceid, videotype, videoname, contentidfromepg, contenttype, playtime from knowyou_ott_ods
//        .ods_uba_cn_videoplayinfo limit 10").show();

        List<Record> records = new ArrayList<>();
        records.add(new Record(1, "张三", 21, "2018"));
        records.add(new Record(2, "李四", 18, "2017"));

        Dataset<Row> ds = ss.createDataFrame(records, Record.class);
        ds.createOrReplaceTempView("records");

        Dataset<Row> df = ss.sql("select * from records");
//        df.show();
        //可以将append改为overwrite，这样如果表已存在会删掉之前的表，新建表
        df.write().mode("overwrite").partitionBy("year").saveAsTable("knowyou_ott_dmt.new_test_partition");

        ss.stop();
    }
}

