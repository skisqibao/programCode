package com.knowyou;


import com.knowyou.util.Config;
import com.knowyou.util.DateUtil;
import com.knowyou.util.UtilConstants;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 付费评估
 */
public class PayPlusRecommend2 {
    private static final Logger log = LoggerFactory.getLogger(PayPlusRecommend2.class);
    //决定是否手动选择日期
    private static Boolean ALS_PAY_DATA_AUTO;
    //原始数据日分区时间
    private static String ALS_PAY_DATA_TIME;
    private static String ALS_PAY_ORDER_DATA_TIME;
    //输入数据数据库
    private static String ALS_PAY_DATABASE;
    //是否切割数据集
    private static Boolean ALS_PAY_IS_SPLIT;

    private static int ALS_PAY_RANK;

    private static int ALS_PAY_NUM_ITERATIONS;

    private static double ALS_PAY_LAMBDA;

    private static double ALS_PAY_ALPHA;

    private static String ALS_PAY_INSERT_SQL;


    public static void main(String[] args) {
        Config config = Config.getInstance();
        ALS_PAY_DATA_AUTO = config.getBoolean(UtilConstants.PayAlsRec.ALS_PAY_DATA_AUTO, false);
        if (ALS_PAY_DATA_AUTO) {
            ALS_PAY_DATA_TIME = DateUtil.getYesterday();
            ALS_PAY_ORDER_DATA_TIME = DateUtil.getOrderYesterday();
        } else {
            ALS_PAY_DATA_TIME = config.getProperty(UtilConstants.PayAlsRec.ALS_PAY_DATA_TIME, "20200921");
            ALS_PAY_ORDER_DATA_TIME = config.getProperty(UtilConstants.PayAlsRec.ALS_PAY_ORDER_DATA_TIME, "2020-12-14");
        }
        ALS_PAY_DATABASE = config.getProperty(UtilConstants.PayAlsRec.ALS_PAY_DATABASE, "default");

        ALS_PAY_IS_SPLIT = config.getBoolean(UtilConstants.PayAlsRec.ALS_PAY_IS_SPLIT, false);

        ALS_PAY_RANK = config.getInt(UtilConstants.PayAlsRec.ALS_PAY_RANK, 50);
        ALS_PAY_NUM_ITERATIONS = config.getInt(UtilConstants.PayAlsRec.ALS_PAY_NUM_ITERATIONS, 10);
        ALS_PAY_LAMBDA = config.getDouble(UtilConstants.PayAlsRec.ALS_PAY_LAMBDA, 0.01);
        ALS_PAY_ALPHA = config.getDouble(UtilConstants.PayAlsRec.ALS_PAY_ALPHA, 0.01);
        ALS_PAY_INSERT_SQL = config.getString(UtilConstants.PayAlsRec.ALS_PAY_INSERT_SQL, "");


        SparkSession ss = SparkSession.builder()
                .appName("PayPlusRecommend")
                .enableHiveSupport()
                .getOrCreate();

        String behaviorDataSql = String.format("select device_id,video_id from %s.ads_rec_userdetail_wtr where dt='%s' ", ALS_PAY_DATABASE, ALS_PAY_DATA_TIME);
        String orderDataSql = String.format("select USER_IDENTITY,PROD_PRC_NAME  from %s.tb_mid_pdt_nettv_user_mon where  deal_date_p='%s' ", ALS_PAY_DATABASE, ALS_PAY_ORDER_DATA_TIME);
        //加载ads_rec_userdetail_wtr输入数据
        JavaRDD<Row> behaviorDataRDD = ss.sql(behaviorDataSql).javaRDD();
        JavaRDD<Row> orderDataRDD = ss.sql(orderDataSql).javaRDD();
        JavaPairRDD<String, String> deviceVideoPairRDD = behaviorDataRDD.mapToPair(new PairFunction<Row, String, String>() {
            public Tuple2<String, String> call(Row row) throws Exception {
                if (2 == row.length()) {
                    String device_id = String.valueOf(row.get(0));
                    String video_id = String.valueOf(row.get(1));
                    return new Tuple2<>(device_id, video_id);
                }
                return null;
            }
        });
        //设备id与产品包的关系
        JavaPairRDD<String, String> deviceProdPairRDD = orderDataRDD.mapToPair(new PairFunction<Row, String, String>() {
            public Tuple2<String, String> call(Row row) throws Exception {
                if (2 == row.length()) {
                    return new Tuple2<>(String.valueOf(row.get(0)), String.valueOf(row.get(1)));
                }
                return null;
            }
        });

        JavaPairRDD<String, String> filterUserProdPairRDD = null;
        JavaPairRDD<String, String> filterUserVideoPairRDD = null;
        //判断是否需要做用户抽样提高调参速度
        if (ALS_PAY_IS_SPLIT) {
            JavaPairRDD<String, Integer> userActivityPairRdd = deviceVideoPairRDD
//                    .union(deviceProdPairRDD)
                    .mapToPair(x -> new Tuple2<>(x._1, 1)).reduceByKey((x, y) -> x + y);
            long userNum = userActivityPairRdd.count();
            System.out.println("抽样播放用户活跃用户数" + userNum);
            List<String> watchSmallUsers = userActivityPairRdd.mapToPair(Tuple2::swap)
                    .sortByKey()
                    .zipWithIndex()
                    .filter(x -> x._2 >= userNum / 2 && x._2 < userNum * 7 / 8)
                    .map(x -> x._1._2)
                    .takeSample(false, 5000, 10);

            System.out.println("watchSmallUsers 抽样用户数" + watchSmallUsers.size());
            List<String> orderSmallUsers = deviceProdPairRDD.map(x -> x._1).distinct().takeSample(false, 5000, 10);
            System.out.println("orderSmallUsers 抽样用户数" + orderSmallUsers.size());

            ArrayList<String> watchSmallUsersList = new ArrayList<>();
            ArrayList<String> orderSmallUsersList = new ArrayList<>();
            for (String watchSmallUser : watchSmallUsers) {
                watchSmallUsersList.add(watchSmallUser);
            }
            for (String orderSmallUser : orderSmallUsers) {
                orderSmallUsersList.add(orderSmallUser);
            }
            watchSmallUsersList.addAll(orderSmallUsersList);
            List<String> smallUsers = watchSmallUsersList.stream().distinct().collect(Collectors.toList());
            System.out.println("smallUsers得size" + smallUsers.size());

            System.out.println("抽样用户设备id list " + smallUsers.subList(0, 10).toString());

            filterUserProdPairRDD = deviceProdPairRDD.filter(x -> smallUsers.contains(x._1));
            System.out.println("deviceProdPairRDD ：" + deviceProdPairRDD.count());
            System.out.println("filterUserProdPairRDD ：" + filterUserProdPairRDD.count());

            filterUserVideoPairRDD = deviceVideoPairRDD.filter(x -> smallUsers.contains(x._1));
            System.out.println("deviceVideoPairRDD ：" + deviceVideoPairRDD.count());
            System.out.println("filterUserVideoPairRDD ：" + filterUserVideoPairRDD.count());
        } else {
            filterUserProdPairRDD = deviceProdPairRDD;
            filterUserVideoPairRDD = deviceVideoPairRDD;
        }

        //获取产品包自增id
        JavaPairRDD<String, Long> prodToIncreasePairRDD = filterUserProdPairRDD.map(x -> x._2).distinct().zipWithIndex();
        //获取产品包最大id
        Long maxProdId = prodToIncreasePairRDD.mapToPair(x -> new Tuple2<>(1, x._2)).reduceByKey((x, y) -> Math.max(x, y)).map(x -> x._2).take(1).get(0);
        System.out.println("产品包最大id为：" + maxProdId);
        //获取节目名自增id
        JavaPairRDD<String, Long> videoToIncreasePairRDD = filterUserVideoPairRDD.map(x -> x._2).distinct().zipWithIndex()
                .mapToPair(x -> new Tuple2<>(x._1, x._2 + 1 + maxProdId));
        //用户，节目，产品
//        JavaPairRDD<String, Tuple2<String, String>> userVideoProd = filterUserVideoPairRDD.fullOuterJoin(filterUserProdPairRDD)
//                .mapToPair(x -> new Tuple2<>(x._1, new Tuple2<>(x._2._1.orElse("null"), x._2._2.orElse("null"))));
        //获取用户自增id
//        JavaPairRDD<String, Long> userIncreasePairRDD = userVideoProd.map(x -> x._1).distinct().zipWithIndex();

        JavaPairRDD<String, Long> userIncreasePairRDD = filterUserVideoPairRDD.union(filterUserProdPairRDD).map(x -> x._1).distinct().zipWithIndex();


        //用户自增id,产品自增id
        JavaPairRDD<Long, Long> userProd = filterUserProdPairRDD.leftOuterJoin(userIncreasePairRDD).mapToPair(x -> new Tuple2<>(x._2._1, x._2._2.orElse(-1L)))
                .leftOuterJoin(prodToIncreasePairRDD).mapToPair(x -> new Tuple2<>(x._2._1, x._2._2.orElse(-1L))).cache();
        //用户自增id,节目自增id
        JavaPairRDD<Long, Long> userVideo = filterUserVideoPairRDD.leftOuterJoin(userIncreasePairRDD).mapToPair(x -> new Tuple2<>(x._2._1, x._2._2.orElse(-1L)))
                .leftOuterJoin(videoToIncreasePairRDD).mapToPair(x -> new Tuple2<>(x._2._1, x._2._2.orElse(-1L))).cache();

        JavaPairRDD<Long, Long> finalTrainRdd = null;
        JavaPairRDD<Long, Long> finalValRdd = null;
        //判断是否需要切割训练集、验证集
        if (ALS_PAY_IS_SPLIT) {
            double[] ratio = {0.8, 0.2};
            JavaRDD<Tuple2<Long, Long>>[] splitRdd = userProd.map(x -> new Tuple2<Long, Long>(x._1, x._2)).randomSplit(ratio, 10);

            //lazy模式导致每次
            JavaRDD<Tuple2<Long, Long>> trainRDD = splitRdd[0].cache();
            JavaRDD<Tuple2<Long, Long>> valRDD = splitRdd[1].cache();
            //得到训练集验证集用户去重自增id
            JavaRDD<Long> trainDistinctUserRdd = trainRDD.map(x -> Long.parseLong(String.valueOf(x._1))).distinct();
            JavaRDD<Long> valDistinctUserRdd = valRDD.map(x -> Long.parseLong(String.valueOf(x._1))).distinct();

            System.out.println("训练集验证集用户去重自增id trainDistinctUserRdd " + trainDistinctUserRdd.count());
            System.out.println("训练集验证集用户去重自增id valDistinctUserRdd " + valDistinctUserRdd.count());

            //得到训练集验证集产品去重自增id
            JavaRDD<Long> trainDistinctProductRdd = trainRDD.map(x -> Long.parseLong(String.valueOf(x._2))).distinct();
            JavaRDD<Long> valDistinctProductRdd = valRDD.map(x -> Long.parseLong(String.valueOf(x._2))).distinct();
            System.out.println("训练集产品去重自增id 数量trainDistinctProductRdd " + trainDistinctProductRdd.count());
            System.out.println("验证集产品去重自增id 数量valDistinctProductRdd " + valDistinctProductRdd.count());

            //得到用户、产品id交集
            JavaRDD<Long> intersectionUserRdd = trainDistinctUserRdd.intersection(valDistinctUserRdd);
            JavaRDD<Long> intersectionProductRdd = trainDistinctProductRdd.intersection(valDistinctProductRdd);
            //得到可用的用户产品交集集合
            List<Long> validUserList = intersectionUserRdd.collect();
            List<Long> validProductList = intersectionProductRdd.collect();
            System.out.println("训练集、验证集用户id交集：" + validUserList.size() + "-----" + validUserList.subList(0, validUserList.size() > 10 ? 10 : validUserList.size()).toString());
            System.out.println("训练集、验证集产品id交集：" + validProductList.size() + "-----" + validProductList.subList(0, validProductList.size() > 10 ? 10 : validProductList.size()).toString());

            ArrayList<Long> validUserArrayList = new ArrayList<>();
            for (Long a : validUserList) {
                validUserArrayList.add(a);
            }
            ArrayList<Long> validProductArrayList = new ArrayList<>();
            for (Long a : validProductList) {
                validProductArrayList.add(a);
            }

            // 得出有效的验证集记录，筛选出用户和节目都存在与训练集中的记录
            finalValRdd = valRDD
                    .filter(x -> {
                        return validUserArrayList.contains(Long.parseLong(String.valueOf(x._1))) && validProductArrayList.contains(Long.parseLong(String.valueOf(x._2)));
                    })
                    .mapToPair(x -> new Tuple2<>(Long.parseLong(String.valueOf(x._1)), Long.parseLong(String.valueOf(x._2))));


            System.out.println("第一遍" + splitRdd[1].count());
            System.out.println("第二遍" + splitRdd[1].count());
            System.out.println("第一遍 cache" + trainRDD.count());
            System.out.println("第二遍 cache" + trainRDD.count());

            JavaRDD<Tuple2<Long, Long>> exchangeRdd = valRDD.filter(x -> {

                return !(validUserArrayList.contains(x._1) && validProductArrayList.contains(x._2));
            });
            JavaRDD<Tuple2<Long, Long>> exchange3Rdd = valRDD.filter(x -> {
//                return !(validUserArrayList.contains(Long.parseLong(String.valueOf(x._1))) && validProductArrayList.contains(Long.parseLong(String.valueOf(x._2))));
                return !(validUserArrayList.contains(x._1) && validProductArrayList.contains(x._2));
            });
            JavaRDD<Tuple2<Long, Long>> exchange2Rdd = valRDD.filter(x -> {
//                return (validUserArrayList.contains(Long.parseLong(String.valueOf(x._1))) && validProductArrayList.contains(Long.parseLong(String.valueOf(x._2))));
                return !(validUserArrayList.contains(x._1) && validProductArrayList.contains(x._2));
            });
            //将无效的验证集记录转移至训练集
            finalTrainRdd = valRDD.filter(x -> {
                return !(validUserArrayList.contains(Long.parseLong(String.valueOf(x._1))) && validProductArrayList.contains(Long.parseLong(String.valueOf(x._2))));
            }).mapToPair(x -> new Tuple2<>(x._1, x._2))
                    .union(trainRDD.mapToPair(x -> new Tuple2<>(x._1, x._2)))
                    .mapToPair(x -> new Tuple2<>(Long.parseLong(String.valueOf(x._1)), Long.parseLong(String.valueOf(x._2))));

            //   <设备自增id,节目自增id,评分>
            JavaRDD<Rating> ratingJavaRDD = finalTrainRdd.union(userVideo).map(x -> new Rating(x._1.intValue(), x._2.intValue(), 1.0));

            System.out.println("splitRdd【0】 数量：" + trainRDD.count());
            System.out.println("splitRdd【1】 数量：" + valRDD.count());
            System.out.println("exchangeRdd 数量：" + exchangeRdd.count());
            System.out.println("exchangeRdd2 数量：" + exchange2Rdd.count());
            System.out.println("exchangeRdd3 数量：" + exchange3Rdd.count());
            System.out.println("finalTrainRdd 数量：" + finalTrainRdd.count());
            System.out.println("finalValRdd 数量：" + finalValRdd.count());
            System.out.println("userProd 数量：" + userProd.count());
            long userProdNum = userProd.map(x -> x._1).distinct().count();
            System.out.println("userProd 去重用户数" + userProdNum);
            System.out.println("userVideo 数量：" + userVideo.count());
            System.out.println("rating RDD数量：" + ratingJavaRDD.count());
            //采用隐式反馈模型
            MatrixFactorizationModel model = ALS.trainImplicit(ratingJavaRDD.rdd(), ALS_PAY_RANK, ALS_PAY_NUM_ITERATIONS, ALS_PAY_LAMBDA, ALS_PAY_ALPHA);

            //得到验证集去重用户id List
//            List<Long> valDistinctUserList = valDistinctUserRdd.map(x -> Long.parseLong(String.valueOf(x))).collect();
            List<Long> valDistinctUserList = finalValRdd.map(x -> x._1).distinct().collect();
            System.out.println("得到验证集去重用户id List size " + valDistinctUserList.size() + "====" + valDistinctUserList.subList(0, valDistinctUserList.size() > 10 ? 10 : valDistinctUserList.size()).toString());

            //得到训练集聚合List
            JavaPairRDD<Long, ArrayList<Long>> trainListPairRDD = finalTrainRdd.groupByKey().mapToPair(x -> {
                ArrayList<Long> prodList = new ArrayList<>();
                Iterator<Long> prodIterator = x._2.iterator();
                while (prodIterator.hasNext()) {
                    prodList.add(prodIterator.next());
                }
                return new Tuple2<Long, ArrayList<Long>>(x._1, prodList);
            });
            JavaPairRDD<Long, ArrayList<Long>> valListPairRDD = finalValRdd.groupByKey().mapToPair(x -> {
                ArrayList<Long> prodList = new ArrayList<>();
                Iterator<Long> prodIterator = x._2.iterator();
                while (prodIterator.hasNext()) {
                    prodList.add(prodIterator.next());
                }
                return new Tuple2<Long, ArrayList<Long>>(x._1, prodList);
            });

            System.out.println("trainListPairRDD去重用户数" + trainListPairRDD.count());
            System.out.println("valListPairRDD去重用户数" + valListPairRDD.count());

            // 获取该用户的观看历史节目列表和实际观看节目列表
            Map<Long, ArrayList<Long>> valHistoryMap = trainListPairRDD.filter(x -> valDistinctUserList.contains(x._1)).collectAsMap();
            Map<Long, ArrayList<Long>> valWatchMap = valListPairRDD.collectAsMap();

            System.out.println("valHistoryMap:" + valHistoryMap.size() + "======");
            System.out.println("valWatchMap:" + valWatchMap.size() + "======");
            double meanF1score = 0;
            double progress = 0;
            for (Long x : valDistinctUserList) {
                ArrayList<Long> history = valHistoryMap.get(x);
                ArrayList<Long> watch = valWatchMap.get(x);
                Rating[] ratings = model.recommendProducts(x.intValue(), 200);
                List<Long> fullResult = new ArrayList<>();
                for (Rating rating : ratings) {
                    if (!history.contains(rating.product()) && rating.product() <= maxProdId) {
                        fullResult.add(Long.parseLong(String.valueOf(rating.product())));
                    }
                }
                List<Long> result = fullResult.subList(0, fullResult.size() > 10 ? 10 : fullResult.size());
                // 计算该用户的f1-score
                int hit = watch.stream().filter(result::contains).collect(Collectors.toList()).size();
                double f1score = 2.0 * hit / (watch.size() + result.size());
                meanF1score += f1score;
                progress += 1;

            }
            System.out.println("------------meanF1score :" + meanF1score);
            System.out.println("------------valDistinctUserList的size :" + valDistinctUserList.size());
            meanF1score /= valDistinctUserList.size();
            System.out.println("--------------------------------------f1-score分数为：" + meanF1score + "--------------------");
            System.out.println("------------------------!!!!!!!!!!!!!!!!!!!!!!!!!!!---------------------------------------");
            log.info("------f1-score分数为：" + meanF1score);

        } else {
            finalTrainRdd = userProd;
            //   <设备自增id,节目自增id,评分>
            JavaRDD<Rating> ratingJavaRDD = finalTrainRdd.union(userVideo).map(x -> new Rating(x._1.intValue(), x._2.intValue(), 1.0));
            //采用隐式反馈模型
            MatrixFactorizationModel model = ALS.trainImplicit(ratingJavaRDD.rdd(), ALS_PAY_RANK, ALS_PAY_NUM_ITERATIONS, ALS_PAY_LAMBDA, ALS_PAY_ALPHA);

            //训练集的笛卡尔积
            JavaRDD<Integer> trainUserRdd = finalTrainRdd.map(x -> x._1.intValue()).distinct().cache();
            JavaRDD<Integer> trainProdRdd = finalTrainRdd.map(x -> x._2.intValue()).distinct().cache();
            System.out.println("训练集用户数" + trainUserRdd.count());
            System.out.println("训练集产品包数" + trainProdRdd.count());


            //用户，产品笛卡尔积
            JavaPairRDD<Integer, Integer> cartesianUserProdRDD = trainUserRdd.cartesian(trainProdRdd).cache();
            trainUserRdd.unpersist();
            trainProdRdd.unpersist();

            System.out.println("笛卡尔积：" + cartesianUserProdRDD.count());

            JavaRDD<Rating> predict = model.predict(cartesianUserProdRDD);
            cartesianUserProdRDD.unpersist();

            //转换为原来的节目名产品包和评分,先翻译用户再翻译产品
            JavaPairRDD<String, Tuple2<String, Double>> finalUserVideoRatingPairRDD = predict.mapToPair(x -> new Tuple2<>(x.user(), new Tuple2<>(x.product(), x.rating())))
                    .leftOuterJoin(userIncreasePairRDD.mapToPair(x -> new Tuple2<>(x._2.intValue(), x._1)))
                    .mapToPair(x -> new Tuple2<>(x._2._1._1, new Tuple2<>(x._2._2.orElse("null"), x._2._1._2)))
                    .leftOuterJoin(prodToIncreasePairRDD.union(videoToIncreasePairRDD).mapToPair(x -> new Tuple2<>(x._2.intValue(), x._1)))
                    .mapToPair(x -> new Tuple2<>(x._2._1._1, new Tuple2<>(x._2._2.orElse("null"), x._2._1._2)));//得到翻译后的用户，产品，评分

            System.out.println("----------时间戳------------------" + ALS_PAY_DATA_TIME);
            String insertTime = ALS_PAY_DATA_TIME.substring(0, 6);
            System.out.println("----------时间戳------------------" + insertTime);
            JavaPairRDD<String, Tuple3<String, Double, String>> stringTuple3JavaPairRDD = finalUserVideoRatingPairRDD
                    .mapToPair(x -> new Tuple2<>(x._1, new Tuple3<>(x._2._1, x._2._2, String.valueOf(insertTime))));
            JavaRDD<Row> finalUserVideoRaingRow = stringTuple3JavaPairRDD.map(x -> RowFactory.create(x._1, x._2._1(), x._2._2(), x._2._3())).cache();
            ArrayList<StructField> finalPayStructFields = new ArrayList<>();
            finalPayStructFields.add(DataTypes.createStructField("device_id", DataTypes.StringType, true));
            finalPayStructFields.add(DataTypes.createStructField("prod_id", DataTypes.StringType, true));
            finalPayStructFields.add(DataTypes.createStructField("rating", DataTypes.DoubleType, true));
            finalPayStructFields.add(DataTypes.createStructField("date_time", DataTypes.StringType, true));
            StructType finalPayStructType = DataTypes.createStructType(finalPayStructFields);
            Dataset<Row> finalPayDF = ss.createDataFrame(finalUserVideoRaingRow, finalPayStructType);
            finalPayDF.createOrReplaceTempView("final_pay");
            //山西写入hive
            ss.sql("select * from final_pay limit 10").show(10);
            ss.sql("set hive.exec.dynamic.partition=true");
            ss.sql("set hive.exec.dynamic.partition.mode=nonstrict");
//            ss.sql("insert into table knowyou_ott.htv_pay_plus_test select * from final_pay ");
            ss.sql(ALS_PAY_INSERT_SQL);
        }
        ss.close();

    }
}
