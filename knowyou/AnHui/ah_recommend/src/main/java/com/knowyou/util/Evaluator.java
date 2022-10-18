package com.knowyou.util;

import org.apache.spark.api.java.JavaPairRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

import static java.util.stream.Collectors.toList;

/**
 * 评价指标
 */
public class Evaluator {
    /**
     * 输入格式为 key: userid value: List[productid, productid, ...]
     * @param valPairRdd
     * @param predictValPairRdd
     * @return
     */
    public static double getF1score(JavaPairRDD<Integer, ArrayList<Integer>> valPairRdd,
                                    JavaPairRDD<Integer, ArrayList<Integer>> predictValPairRdd) {
        // 计算验证集用户各自的f1-score
        JavaPairRDD<Integer, Double> f1PairRdd = valPairRdd.join(predictValPairRdd).mapToPair(v1 -> {
            List<Integer> valList = v1._2._1;
            List<Integer> predictList = v1._2._2;
            // 求交集 命中数
            int hit = valList.stream().filter(predictList::contains).collect(toList()).size();
            // 求f1-score = 2 * hit / (实际观看数量 + 预测数量)
            double f1 = 2.0 * hit / (valList.size() + predictList.size());
            return new Tuple2<>(v1._1, f1);
        });

        System.out.println("验证集用户f1-score-----------------------------------");
        f1PairRdd.sortByKey().take(20).forEach(x -> System.out.println(x));

        // 求平均f1-score
        return f1PairRdd.mapToDouble(v1 -> v1._2).mean();
    }
}
