package com.knowyou.map;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

import java.io.Serializable;

public class PairToRatingFunc implements Function<Tuple2<Long, Tuple2>, Rating>, Serializable {
    @Override
    public Rating call(Tuple2<Long, Tuple2> v1) throws Exception {
        int pid = v1._1.intValue();
        int userId = Integer.parseInt(String.valueOf(v1._2._1));
//        String catalog = String.valueOf(v1._2._2);
//        return new Rating(userId,pid,1.0);

        String catalog = String.valueOf(v1._2._2).split("#")[0];
        double rating = Double.parseDouble(String.valueOf(v1._2._2).split("#")[1]);
        return new Rating(userId, pid, rating);
    }
}
