package com.knowyou.map;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import scala.Option;
import scala.Tuple2;

import java.io.Serializable;

/**
 *  <seriesheadcode,<userid,catalog>> ->  <pid,<userid,catalog>>
 */
public class VideoPairJoinFunc implements
        Serializable, PairFunction<Tuple2<String, Tuple2<Tuple2, Long>>, Long, Tuple2> {
    @Override
    public Tuple2<Long, Tuple2> call(Tuple2<String, Tuple2<Tuple2, Long>> v1) throws Exception {
        String seriesheadcode = v1._1;
        long pid = v1._2._2;
        String userId = String.valueOf(v1._2._1._1);
        String catalog = String.valueOf(v1._2._1._2);
        return new Tuple2<>(pid,new Tuple2(userId,catalog));
    }

}
