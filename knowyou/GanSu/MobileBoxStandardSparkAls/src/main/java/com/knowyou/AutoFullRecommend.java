package com.knowyou;

import com.alibaba.fastjson.JSONObject;
import com.knowyou.util.Config;
import com.knowyou.util.HbaseUtil;
import com.knowyou.util.UtilConstants;
import com.knowyou.util.DateUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple4;
import scala.reflect.ClassManifestFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 标准版猜你喜欢ALS算法程序
 */
public class AutoFullRecommend {
    private static final Logger log = LoggerFactory.getLogger(AutoFullRecommend.class);
    // 潜因子数量
    private static int ALS_RANK;
    // ALS算法迭代次数
    private static int ALS_NUM_ITERATIONS;
    // LAMBDA参数
    private static Double ALS_LAMBDA;
    // 节目相似度算法为单机运算，为避免内存溢出，需要分块运算，每块的大小设定
    private static int ALS_BATCH_SIZE;
    // 相关推荐潜因子数量
    private static int ALS_RELETIVE_RANK;
    //猜你喜欢推荐节目数
    private static int ALS_REC_NUM;
    //所有牌照方
    private static String ALS_LICENSE;
    //原始数据日分区时间
    private static String ALS_DATA_TIME;
    //ALS算法置信参数
    private static Double ALS_ALPHA;
    //ALS模型HDFS保存路径
    private static String ALS_MODEL_HDFS_PATH;
    //用户与自增id关系rdd存储目录
    private static String USER_TRANSLATE_HDFS_PATH;
    //输入数据数据库
    private static String ALS_DATABASE;
    //决定是否手动选择日期
    private static String ALS_DATA_AUTO;
    //als hbase表列簇
    private static final String ALS_COLUMN_FAMILY_NAME = "info";

    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "root");

        Config config = Config.getInstance();
        ALS_RANK = config.getInt(UtilConstants.AlsRec.ALS_RANK, 200);
        ALS_NUM_ITERATIONS = config.getInt(UtilConstants.AlsRec.ALS_NUM_ITERATIONS, 10);
        ALS_LAMBDA = config.getDouble(UtilConstants.AlsRec.ALS_LAMBDA, 0.01);
        ALS_BATCH_SIZE = config.getInt(UtilConstants.AlsRec.ALS_BATCH_SIZE, 500);
        ALS_RELETIVE_RANK = config.getInt(UtilConstants.AlsRec.ALS_RELETIVE_RANK, 20);
        ALS_REC_NUM = config.getInt(UtilConstants.AlsRec.ALS_REC_NUM, 50);
        ALS_LICENSE = config.getProperty(UtilConstants.AlsRec.ALS_LICENSE, "3");
        ALS_DATA_AUTO = config.getProperty(UtilConstants.AlsRec.ALS_DATA_AUTO, "true");

        if ("true".equals(ALS_DATA_AUTO)) {
            ALS_DATA_TIME = DateUtil.getTheDayBeforeYesterday();
        } else {
            ALS_DATA_TIME = config.getProperty(UtilConstants.AlsRec.ALS_DATA_TIME, "20200921");
        }

        if (args.length == 1) {
            ALS_DATA_TIME = args[0];
        }
        System.out.println("AutoFullRecommend --> Current date : [" + ALS_DATA_TIME + "]");

        ALS_ALPHA = config.getDouble(UtilConstants.AlsRec.ALS_ALPHA, 0.1);
        ALS_MODEL_HDFS_PATH = config.getProperty(UtilConstants.AlsRec.ALS_MODEL_HDFS_PATH, "0");
        USER_TRANSLATE_HDFS_PATH = config.getProperty(UtilConstants.AlsRec.USER_TRANSLATE_HDFS_PATH, "0");
        ALS_DATABASE = config.getProperty(UtilConstants.AlsRec.ALS_DATABASE, "default");


        SparkSession ss = SparkSession.builder()
                .appName("AutoFullRecommend")
                .config("spark.hadoop.validateOutputSpecs", "false")
                .enableHiveSupport()
                .getOrCreate();
        ss.sparkContext().setLogLevel("INFO");
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(ss.sparkContext());

        //切分牌照方，遍历牌照方根据不同数据源进行算法模型计算
        String[] licenseSplit = ALS_LICENSE.split(",");
        int licenseNum = licenseSplit.length;
        for (int n = 0; n < licenseNum; n++) {
            String licenseName = licenseSplit[n];

            String extractDataSql = String
                    .format("select device_id, video_name  from %s.ads_rec_userdetail_wtr " +
                                    "where license ='%s' and dt='%s' and length(device_id) > 0 and length(video_name) >" +
                                    " 0 group by device_id, video_name", ALS_DATABASE,
                            licenseName, ALS_DATA_TIME);

            System.out.println(extractDataSql);

            String watchHistorySql = String
                    .format("select device_id, concat_ws('###',collect_set(concat_ws('_',video_name,last_watch_time))) " +
                                    "watch_history, dt " +
                                    "from %s.ads_rec_userdetail_wtr where license ='%s' and dt='%s' and length" +
                                    "(device_id) > 0 and length(video_name) > 0 group by " +
                                    "device_id, dt ",
                            ALS_DATABASE, licenseName, ALS_DATA_TIME);


            //加载用户观看历史数据
            JavaRDD<Row> watchHistoryRDD = ss.sql(watchHistorySql).javaRDD();
            JavaPairRDD<String, Tuple2<String, String>> watchHistoryJavaPairRDD =
                    watchHistoryRDD.mapToPair(new PairFunction<Row, String, Tuple2<String, String>>() {
                        @Override
                        public Tuple2<String, Tuple2<String, String>> call(Row row) throws Exception {
                            String device_id = String.valueOf(row.get(0));
                            String watch_history = String.valueOf(row.get(1));
                            String dt = String.valueOf(row.get(2));
                            return new Tuple2<>(device_id, new Tuple2<>(watch_history, dt));
                        }
                    });

            watchHistoryJavaPairRDD.foreach(new VoidFunction<Tuple2<String, Tuple2<String, String>>>() {
                @Override
                public void call(Tuple2<String, Tuple2<String, String>> v1) throws Exception {
                    String rowkey = String.valueOf(v1._1);
                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put("watchvideoid", v1._2._1);
                    jsonObject.put("traindate", v1._2._2);
                    String[] column_list = {"watchhistory"};
                    String[] value_list = {jsonObject.toString()};
                    String tableName = "rec_guess_history_" + licenseName;
                    try {
                        HbaseUtil.put(rowkey, tableName, ALS_COLUMN_FAMILY_NAME, column_list, value_list);
                    } catch (Exception e) {
                        log.error(tableName + " hbase insert error");
                    }
                }
            });

            //加载ads_rec_userdetail_wtr输入数据源
            JavaRDD<Row> rawDataRDD = ss.sql(extractDataSql).javaRDD();

            JavaPairRDD<String, String> deviceVideoPairRDD = rawDataRDD.mapToPair(new PairFunction<Row, String,
                    String>() {
                public Tuple2<String, String> call(Row row) throws Exception {
                    if (2 == row.length()) {
                        String device_id = String.valueOf(row.get(0));
                        String video_id = String.valueOf(row.get(1));
                        return new Tuple2<String, String>(device_id, video_id);
                    }
                    return null;
                }
            }).cache();

            //用户最大观看节目数
            JavaPairRDD<Integer, Integer> userHistoryMaxPairRDD = deviceVideoPairRDD.mapToPair(x -> new Tuple2<>(x._1
                    , 1))
                    .reduceByKey(new Function2<Integer, Integer, Integer>() {
                        @Override
                        public Integer call(Integer v1, Integer v2) throws Exception {
                            return v1 + v2;
                        }
                    }).mapToPair(x -> new Tuple2<>(1, x._2))
                    .reduceByKey(new Function2<Integer, Integer, Integer>() {
                        @Override
                        public Integer call(Integer v1, Integer v2) throws Exception {
                            if (v2 > v1) {
                                return v2;
                            }
                            return v1;
                        }
                    });

            final Integer[] historyMax = {0};
/*            userHistoryMaxPairRDD.foreach(x -> {
                historyMax[0] = x._2;
            });*/

            JavaPairRDD<String, Long> deviceIncreaseJavaPairRDD =
                    deviceVideoPairRDD.map(x -> x._1).distinct().zipWithIndex();
            JavaPairRDD<String, Long> videoIncreaseJavaPairRDD =
                    deviceVideoPairRDD.map(x -> x._2).distinct().zipWithIndex();
            JavaPairRDD<Integer, String> idVideoPairRDD =
                    videoIncreaseJavaPairRDD.mapToPair(x -> new Tuple2<>(x._2.intValue(), x._1));
            JavaPairRDD<Integer, String> idDevicePairRDD =
                    deviceIncreaseJavaPairRDD.mapToPair(x -> new Tuple2<>(x._2.intValue(), x._1));

            deviceIncreaseJavaPairRDD.map(x -> x._1 + "," + x._2).saveAsTextFile(String.format(USER_TRANSLATE_HDFS_PATH, licenseName));

            //将原始id转化为自增id
            JavaPairRDD<Integer, Integer> increaseDeviceVideoPairRDD =
                    deviceVideoPairRDD.join(deviceIncreaseJavaPairRDD)
                            .mapToPair(new PairFunction<Tuple2<String, Tuple2<String, Long>>, String, Long>() {
                                @Override
                                public Tuple2<String, Long> call(Tuple2<String, Tuple2<String, Long>> stringTuple2Tuple2) throws Exception {
                                    //Tuple2<节目id,设备自增id>
                                    return new Tuple2<>(stringTuple2Tuple2._2._1, stringTuple2Tuple2._2._2);
                                }
                            }).join(videoIncreaseJavaPairRDD)
                            .mapToPair(new PairFunction<Tuple2<String, Tuple2<Long, Long>>, Long, Long>() {
                                @Override
                                public Tuple2<Long, Long> call(Tuple2<String, Tuple2<Long, Long>> stringTuple2Tuple2) throws Exception {
                                    //Tuple2<设备自增id,节目自增id>
                                    return new Tuple2<>(stringTuple2Tuple2._2._1, stringTuple2Tuple2._2._2);
                                }
                            }).mapToPair(x -> new Tuple2<>(x._1.intValue(), x._2.intValue()));

            deviceVideoPairRDD.unpersist();

            //获得用户，观看记录Arraylist
            JavaPairRDD<Integer, String> deviceUser =
                    increaseDeviceVideoPairRDD.mapToPair(x -> new Tuple2<>(x._1.toString(), x._2.toString()))
                            .reduceByKey(new Function2<String, String, String>() {
                                @Override
                                public String call(String v1, String v2) throws Exception {
                                    return v1 + "###" + v2;
                                }
                            }).mapToPair(x -> new Tuple2<>(Integer.parseInt(x._1), x._2));

            JavaRDD<Rating> ratingJavaRDD = increaseDeviceVideoPairRDD.map(new Function<Tuple2<Integer, Integer>,
                    Rating>() {
                @Override
                public Rating call(Tuple2<Integer, Integer> longLongTuple2) throws Exception {
                    //用户自增id,节目自增id,评分
                    return new Rating(longLongTuple2._1, longLongTuple2._2, 1.0);
                }
            });

//            System.out.println("ratingRDD分区数：" + ratingJavaRDD.getNumPartitions() +
//                    "=========================================================================");

            if (ratingJavaRDD.count() == 0) {
                continue;
            }
            //采用隐式反馈模型
            MatrixFactorizationModel model = ALS.trainImplicit(ratingJavaRDD.rdd(), ALS_RANK, ALS_NUM_ITERATIONS,
                    ALS_LAMBDA, ALS_ALPHA);

            Broadcast<Integer> broadcast = ss.sparkContext().broadcast(ALS_REC_NUM,
                    ClassManifestFactory.classType(int.class));

            JavaPairRDD<Integer, Rating[]> predictRdd =
                    model.recommendProductsForUsers(ALS_REC_NUM + historyMax[0]).toJavaRDD()
                            .mapToPair(v1 -> new Tuple2<>((Integer) v1._1, v1._2));

            // 向每个用户推荐Top50节目（去除观看过的节目）
            JavaPairRDD<Object, Rating[]> recommendsRDD = predictRdd
                    .leftOuterJoin(deviceUser)
                    .mapToPair(v1 -> {
                        ArrayList<Rating> ratingList = new ArrayList<>();
                        Rating[] ratings = v1._2._1;
                        String watchHistoryString = v1._2._2.orElse("unknown");
                        String[] videoIds = watchHistoryString.split("###");
                        ArrayList<String> strings = new ArrayList<>(Arrays.asList(videoIds));

                        int count = 0;
                        for (Rating r : ratings) {
                            if (!strings.contains(String.valueOf(r.product()))) {
                                ratingList.add(r);
                                count++;
                                if (count >= broadcast.value()) {
                                    break;
                                }
                            }
                        }

                        Rating[] ratingResult = new Rating[ratingList.size()];

                        ratingList.toArray(ratingResult);

                        return new Tuple2<>(v1._1, ratingResult);
                    });

            // 将ALS推荐的Top节目id列表转换为2个字段：<用户原始id，"{\"video_id\":" + videoid + ",\"order\":" + ranks + ",\"rating\":" +
            // rating + "\"}">
            JavaPairRDD<String, String> topNRDD = recommendsRDD
                    .flatMap(x -> {
                        //得到评价结果
                        Rating[] ratings = x._2;
                        //Tuple4<设备自增id,节目自增id,排名,评分>
                        List<Tuple4<Integer, Integer, Integer, Double>> records = new ArrayList<>();
                        for (int i = 0; i < ratings.length; i++) {
                            records.add(new Tuple4<>(Integer.parseInt(x._1.toString()), ratings[i].product(), i + 1,
                                    ratings[i].rating()));
                        }
                        return records.iterator();
                    })
                    .mapToPair(x -> {
                        Integer increaseDeviceId = x._1(); //数字设备id
                        Integer increaseVideoId = x._2(); //数字节目id
                        Integer ranks = x._3();     //排名
                        Double rating = x._4();     //评分
                        return new Tuple2<>(increaseVideoId, new Tuple4<>(increaseDeviceId, increaseVideoId, ranks,
                                rating));
                    })
                    .leftOuterJoin(idVideoPairRDD)
                    .mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Tuple4<Integer, Integer, Integer, Double>,
                            Optional<String>>>, Integer, Tuple4<String, Integer, Integer, Double>>() {
                        @Override
                        public Tuple2<Integer, Tuple4<String, Integer, Integer, Double>> call(Tuple2<Integer,
                                Tuple2<Tuple4<Integer, Integer, Integer, Double>, Optional<String>>> v1) throws Exception {
                            //获得原始节目id,得不到则置为0
                            String videoid = v1._2._2.orElse("0");
                            //获得设备自增id
                            Integer increaseDeviceId = v1._2._1._1();
                            //获得节目自增id
                            Integer increaseVideoId = v1._2._1._2();
                            //获得排名
                            Integer ranks = v1._2._1._3();
                            //获得评分
                            Double rating = v1._2._1._4();
                            return new Tuple2<>(increaseDeviceId,
                                    new Tuple4<String, Integer, Integer, Double>(videoid, increaseVideoId, ranks,
                                            rating));
                        }
                    })
                    .leftOuterJoin(idDevicePairRDD)
                    .map(new Function<Tuple2<Integer, Tuple2<Tuple4<String, Integer, Integer, Double>,
                            Optional<String>>>, Tuple2<String, String>>() {
                        @Override
                        public Tuple2<String, String> call(Tuple2<Integer, Tuple2<Tuple4<String, Integer, Integer,
                                Double>, Optional<String>>> v1) throws Exception {
                            //获得设备原始id,得不到就置为unknown
                            String deviceid = v1._2._2.orElse("unknown");
                            //获得节目原始id
                            String videoid = v1._2._1._1();
                            //获得节目自增id
                            Integer increaseVideoId = v1._2._1._2();
                            //获得排名
                            Integer ranks = v1._2._1._3();
                            //获得评分
                            Double rating = v1._2._1._4();
                            String a =
                                    "{\"video_id\": \"" + videoid + "\",\"order\":" + ranks + ",\"rating\":\"" + rating + "\"}";
                            return new Tuple2<String, String>(deviceid, a);
                        }
                    })
                    .mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
                        @Override
                        public Tuple2<String, String> call(Tuple2<String, String> v1) throws Exception {
                            return new Tuple2<>(v1._1, v1._2);
                        }
                    })
                    .reduceByKey((Function2<String, String, String>) (v1, v2) -> v1 + "," + v2);

            topNRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
                @Override
                public void call(Tuple2<String, String> data) throws Exception {
                    String rowkey = String.valueOf(data._1);
                    String value = "[" + data._2 + "]";
                    String[] column_list = {"recommend"};
                    String[] value_list = {value};
                    String tableName = "rec_guess_" + licenseName;
                    try {
                        HbaseUtil.put(rowkey, tableName, ALS_COLUMN_FAMILY_NAME, column_list, value_list);
                    } catch (Exception e) {
                        log.error(tableName + " hbase insert error");
                    }
                }
            });

            //模型保存
            model.save(jsc.sc(), String.format(ALS_MODEL_HDFS_PATH, licenseName));


        }
        jsc.close();
        ss.close();

    }
}
