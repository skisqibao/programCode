package com.knowyou.Scheduler;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import scala.Tuple3;
import scala.Tuple4;

import java.util.ArrayList;
import java.util.List;

public class ALSRecTranslate {

    /**
     * @param userRecRDD             用户自增id，节目自增id，分数，推荐排名
     * @param stringLongUserPairRDD
     * @param stringLongVideoPairRDD
     * @return 用户原始id，节目原始id，分数，推荐排名
     */
    public Dataset<Row> getTranslate(SparkSession ss, JavaRDD<Tuple4<Integer, Integer, Double, Integer>> userRecRDD,
                                     JavaPairRDD<String, Long> stringLongUserPairRDD,
                                     JavaPairRDD<String, Long> stringLongVideoPairRDD) {
        JavaPairRDD<Integer, String> intStringUserPairRDD = stringLongUserPairRDD.mapToPair(x -> new Tuple2<>(x._2.intValue(), x._1));
        JavaPairRDD<Integer, String> intStringVideoPairRDD = stringLongVideoPairRDD.mapToPair(x -> new Tuple2<>(x._2.intValue(), x._1));

        JavaRDD<Row> recRowRDD = userRecRDD.mapToPair(x -> new Tuple2<>(x._1(), new Tuple3(x._2(), x._3(), x._4())))
                .join(intStringUserPairRDD)
                .mapToPair(x -> new Tuple2<>(Integer.valueOf(String.valueOf(x._2._1._1())),
                        new Tuple3<>(x._2._2, Double.valueOf(String.valueOf(x._2._1._2())),
                                Integer.valueOf(String.valueOf(x._2._1._3())))))
                .join(intStringVideoPairRDD)
                .map(x -> RowFactory.create(String.valueOf(x._2._1._1()), String.valueOf(x._2._2), x._2._1._2(), x._2._1._3()));

        List<StructField> recStructFields = new ArrayList<>();
        recStructFields.add(DataTypes.createStructField("deviceid", DataTypes.StringType, true));
        recStructFields.add(DataTypes.createStructField("videoid", DataTypes.StringType, true));
        recStructFields.add(DataTypes.createStructField("rating", DataTypes.DoubleType, true));
        recStructFields.add(DataTypes.createStructField("rank", DataTypes.IntegerType, true));
        StructType userStructType = DataTypes.createStructType(recStructFields);
        Dataset<Row> userDataFrame = ss.createDataFrame(recRowRDD, userStructType);

        return userDataFrame;
    }


}
