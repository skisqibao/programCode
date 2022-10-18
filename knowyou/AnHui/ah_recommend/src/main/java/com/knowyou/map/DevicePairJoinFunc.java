package com.knowyou.map;

import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

/**
 * <deviceid, <<seriesheadcode,catalog>, userid>>  ->  <seriesheadcode,<userid,catalog>>
 *
 */
public class DevicePairJoinFunc implements PairFunction<Tuple2<String, Tuple2<Tuple2, Long>>, String, Tuple2>,
        Serializable {
    @Override
    public Tuple2<String, Tuple2> call(Tuple2<String, Tuple2<Tuple2, Long>> v1) throws Exception {
        String deviceid = v1._1;
        Long userId =  v1._2._2;
        String seriesheadcode = String.valueOf(v1._2._1._1);
        String catalog = String.valueOf(v1._2._1._2);
        return new Tuple2<>(seriesheadcode,new Tuple2(userId,catalog));
    }

}
