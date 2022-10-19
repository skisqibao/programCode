package com.knowyou;

import com.alibaba.fastjson.JSONObject;
import com.knowyou.util.Config;
import com.knowyou.util.HbaseUtil;
import com.knowyou.util.UtilConstants;
import com.knowyou.util.DateUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.ml.linalg.DenseMatrix;
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
import scala.Tuple4;

import java.util.*;
import java.util.stream.Collectors;

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
    private static Boolean ALS_DATA_AUTO;
    //als hbase表列簇
    private static final String ALS_COLUMN_FAMILY_NAME = "info";


    public static void main(String[] args) {

        Config config = Config.getInstance();
        ALS_RANK = config.getInt(UtilConstants.AlsRec.ALS_RANK, 20);
        ALS_NUM_ITERATIONS = config.getInt(UtilConstants.AlsRec.ALS_NUM_ITERATIONS, 10);
        ALS_LAMBDA = config.getDouble(UtilConstants.AlsRec.ALS_LAMBDA, 0.01);
        ALS_BATCH_SIZE = config.getInt(UtilConstants.AlsRec.ALS_BATCH_SIZE, 500);
        ALS_RELETIVE_RANK = config.getInt(UtilConstants.AlsRec.ALS_RELETIVE_RANK, 20);
        ALS_REC_NUM = config.getInt(UtilConstants.AlsRec.ALS_REC_NUM, 50);
        ALS_LICENSE = config.getProperty(UtilConstants.AlsRec.ALS_LICENSE, "3");
        ALS_DATA_AUTO = config.getBoolean(UtilConstants.AlsRec.ALS_DATA_AUTO, false);
        if (ALS_DATA_AUTO) {
            ALS_DATA_TIME = DateUtil.getYesterday();
        } else {
            ALS_DATA_TIME = config.getProperty(UtilConstants.AlsRec.ALS_DATA_TIME, "20200921");
        }
        ALS_ALPHA = config.getDouble(UtilConstants.AlsRec.ALS_ALPHA, 0.1);
        ALS_MODEL_HDFS_PATH = config.getProperty(UtilConstants.AlsRec.ALS_MODEL_HDFS_PATH, "0");
        USER_TRANSLATE_HDFS_PATH = config.getProperty(UtilConstants.AlsRec.USER_TRANSLATE_HDFS_PATH, "0");
        ALS_DATABASE = config.getProperty(UtilConstants.AlsRec.ALS_DATABASE, "default");


        SparkSession ss = SparkSession.builder()
                .appName("AutoFullRecommend")
                .enableHiveSupport()
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(ss.sparkContext());

        //切分牌照方，遍历牌照方根据不同数据源进行算法模型计算
        String[] licenseSplit = ALS_LICENSE.split(",");
        int licenseNum = licenseSplit.length;
        for (int n = 0; n < licenseNum; n++) {
            String licenseName = licenseSplit[n];
            String extractDataSql = String
                    .format("select device_id ,video_id  from %s.ads_rec_userdetail_wtr " +
                            "where license ='%s' and dt='%s'", ALS_DATABASE, licenseName, ALS_DATA_TIME);

            String watchHistorySql = String
                    .format("select device_id,concat_ws(',',collect_set(concat_ws('_',video_id,last_watch_time))) watch_history,dt " +
                            "from %s.ads_rec_userdetail_wtr  where license ='%s' and dt='%s' group by device_id,dt", ALS_DATABASE, licenseName, ALS_DATA_TIME);

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
                    String tableName = "hlwdsyyzsns1:rec_guess_history_" + licenseName;
                    try {
                        HbaseUtil.put(rowkey, tableName, ALS_COLUMN_FAMILY_NAME, column_list, value_list);
                    } catch (Exception e) {
                        log.error(tableName + " hbase insert error");
                    }
                }
            });
            //加载ads_rec_userdetail_wtr输入数据
            JavaRDD<Row> rawDataRDD = ss.sql(extractDataSql).javaRDD();

            JavaPairRDD<String, String> deviceVideoPairRDD = rawDataRDD.mapToPair(new PairFunction<Row, String, String>() {
                public Tuple2<String, String> call(Row row) throws Exception {
                    if (2 == row.length()) {
                        String device_id = String.valueOf(row.get(0));
                        String video_id = String.valueOf(row.get(1));
                        return new Tuple2<String, String>(device_id, video_id);
                    }
                    return null;
                }
            });

            //获得用户，历史观看记录Arraylist
            JavaPairRDD<String, String> deviceUser = deviceVideoPairRDD.reduceByKey(new Function2<String, String, String>() {
                @Override
                public String call(String s, String s2) throws Exception {
                    return s + "," + s2;
                }
            });


            //用户观看历史最大观看节目数
            JavaPairRDD<Integer, Integer> userHistoryMaxPairRDD = deviceVideoPairRDD.mapToPair(x -> new Tuple2<>(x._1, 1))
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
                            } else {
                                return v1;
                            }
                        }
                    });
            Integer historyMax = 0;

            List<Tuple2<Integer, Integer>> take = userHistoryMaxPairRDD.collect();
            if (take != null && take.size() > 0) {
                historyMax = take.get(0)._2;
            } else {
                System.out.println("===============userHistoryMaxPairRDD is null=============");
            }

            JavaPairRDD<String, Long> deviceIncreaseJavaPairRDD = deviceVideoPairRDD.map(x -> x._1).distinct().zipWithIndex();
            JavaPairRDD<String, Long> videoIncreaseJavaPairRDD = deviceVideoPairRDD.map(x -> x._2).distinct().zipWithIndex();
            JavaPairRDD<Integer, String> idVideoPairRDD = videoIncreaseJavaPairRDD.mapToPair(x -> new Tuple2<>(x._2.intValue(), x._1));
            JavaPairRDD<Integer, String> idDevicePairRDD = deviceIncreaseJavaPairRDD.mapToPair(x -> new Tuple2<>(x._2.intValue(), x._1));
//            String userTranslateHdfsPath = String.format(USER_TRANSLATE_HDFS_PATH, licenseName);
//            deviceIncreaseJavaPairRDD.map(x -> x._1 + "," + x._2).repartition(4).saveAsTextFile(userTranslateHdfsPath);
            deviceIncreaseJavaPairRDD.map(x -> x._1 + "," + x._2).saveAsTextFile(String.format(USER_TRANSLATE_HDFS_PATH, licenseName));

            //将原始id转化为自增id
            JavaPairRDD<Long, Long> increaseDeviceVideoPairRDD = deviceVideoPairRDD.join(deviceIncreaseJavaPairRDD)
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
                    });
            //   <设备自增id,节目自增id,评分>
            JavaRDD<Rating> ratingJavaRDD = increaseDeviceVideoPairRDD.map(new Function<Tuple2<Long, Long>, Rating>() {
                @Override
                public Rating call(Tuple2<Long, Long> longLongTuple2) throws Exception {
                    //用户自增id,节目自增id,评分
                    return new Rating(longLongTuple2._1.intValue(), longLongTuple2._2.intValue(), 1.0);
                }
            });


            JavaPairRDD<String, String> deviceVideoIncreasePairRdd = increaseDeviceVideoPairRDD.mapToPair(new PairFunction<Tuple2<Long, Long>, String, String>() {
                @Override
                public Tuple2<String, String> call(Tuple2<Long, Long> longLongTuple2) throws Exception {
                    return new Tuple2<>(String.valueOf(longLongTuple2._1), String.valueOf(longLongTuple2._2));
                }
            }).reduceByKey(new Function2<String, String, String>() {
                @Override
                public String call(String s, String s2) throws Exception {
                    return s + "," + s2;
                }
            });


            //采用隐式反馈模型
            MatrixFactorizationModel model = ALS.trainImplicit(ratingJavaRDD.rdd(), ALS_RANK, ALS_NUM_ITERATIONS, ALS_LAMBDA, ALS_ALPHA);

            // 向每个用户推荐Top50节目（去除观看过的节目）

            JavaPairRDD<Object, Rating[]> recommendsRDD = model.recommendProductsForUsers(ALS_REC_NUM + 50).toJavaRDD()
                    .mapToPair(v1 -> new Tuple2<>(String.valueOf(v1._1), v1._2))
                    .leftOuterJoin(deviceVideoIncreasePairRdd)
                    .mapToPair(v1 -> {
                        //new结果list
                        ArrayList<Rating> ratingList = new ArrayList<>();
                        Rating[] ratings = v1._2._1;

                        ArrayList<Rating> rawRating = new ArrayList<>(Arrays.asList(ratings));
                        List<Rating> sortRatingList = rawRating.stream()
                                .sorted(Comparator.comparing(Rating::rating).reversed()).collect(Collectors.toList());

                        String[] watchHistory = v1._2._2.orElse("unknown").split(",");
                        ArrayList<String> watchHistoryList = new ArrayList<>(Arrays.asList(watchHistory));

                        for (int i = 0; i < sortRatingList.size(); i++) {
                            if (!watchHistoryList.contains(String.valueOf(sortRatingList.get(i).product()))) {
                                ratingList.add(sortRatingList.get(i));
                                if (ratingList.size() == 20) {
                                    break;
                                }
                            }
                        }

                        Rating[] ratingResult = ratingList.toArray(new Rating[ratingList.size()]);

                        return new Tuple2<>(v1._1, ratingResult);
                    });
            // 将ALS推荐的Top节目id列表转换为2个字段：<用户原始id，"{\"video_id\":" + videoid + ",\"order\":" + ranks + ",\"rating\":" + rating + "\"}">
            JavaPairRDD<String, String> topNRDD = recommendsRDD
                    .flatMap(x -> {
                        //得到评价结果
                        Rating[] ratings = x._2;
                        //Tuple4<设备自增id,节目自增id,排名,评分>
                        List<Tuple4<Integer, Integer, Integer, Double>> records = new ArrayList<>();
                        for (int i = 0; i < ratings.length; i++) {
                            records.add(new Tuple4<>(Integer.parseInt(x._1.toString()), ratings[i].product(), i + 1, ratings[i].rating()));
                        }
                        return records.iterator();
                    })
                    .mapToPair(x -> {
                        Integer increaseDeviceId = x._1(); //数字设备id
                        Integer increaseVideoId = x._2(); //数字节目id
                        Integer ranks = x._3();     //排名
                        Double rating = x._4();     //评分
                        return new Tuple2<>(increaseVideoId, new Tuple4<>(increaseDeviceId, increaseVideoId, ranks, rating));
                    })
                    .leftOuterJoin(idVideoPairRDD)
                    .mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Tuple4<Integer, Integer, Integer, Double>, Optional<String>>>, Integer, Tuple4<String, Integer, Integer, Double>>() {
                        @Override
                        public Tuple2<Integer, Tuple4<String, Integer, Integer, Double>> call(Tuple2<Integer, Tuple2<Tuple4<Integer, Integer, Integer, Double>, Optional<String>>> v1) throws Exception {
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
                            return new Tuple2<>(increaseDeviceId, new Tuple4<String, Integer, Integer, Double>(videoid, increaseVideoId, ranks, rating));
                        }
                    })
                    .leftOuterJoin(idDevicePairRDD)
                    .mapToPair(new PairFunction<Tuple2<Integer, Tuple2<Tuple4<String, Integer, Integer, Double>, Optional<String>>>, String, String>() {

                        @Override
                        public Tuple2<String, String> call(Tuple2<Integer, Tuple2<Tuple4<String, Integer, Integer, Double>, Optional<String>>> v1) throws Exception {
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

                            final JSONObject json_rec = new JSONObject();
                            json_rec.put("videoid", videoid);
                            json_rec.put("order", ranks);
                            json_rec.put("rating", rating);
//                            String a = "{\"video_id\": \"" + videoid + "\",\"order\":" + ranks + ",\"rating\":\"" + rating + "\"}";
                            return new Tuple2<>(deviceid, json_rec.toJSONString());
                        }
                    })
                    .reduceByKey((Function2<String, String, String>) (v1, v2) -> v1 + "," + v2);

            topNRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<String, String>>>() {
                @Override
                public void call(Iterator<Tuple2<String, String>> tuple2Iterator) throws Exception {
                    while (tuple2Iterator.hasNext()) {
                        String tableName = "hlwdsyyzsns1:rec_guess_" + licenseName;
                        try {
                            Tuple2<String, String> data = tuple2Iterator.next();
                            String rowkey = String.valueOf(data._1);
                            String value = "[" + data._2 + "]";
                            if (StringUtils.isEmpty(rowkey) || "unknown".equals(rowkey)) {
                                rowkey = "0";
                                value = "[{\"video_id\": \"0\"]";
                            }
                            if (StringUtils.isEmpty(value)) {
                                value = "[{\"video_id\": \"0\"]";
                            }
                            String[] column_list = {"recommend"};
                            String[] value_list = {value};
                            HbaseUtil.put(rowkey, tableName, ALS_COLUMN_FAMILY_NAME, column_list, value_list);
                        } catch (Exception e) {
                            log.error(tableName + " hbase insert error");
                        }
                    }
                }
            });
            //模型保存
            model.save(jsc.sc(), String.format(ALS_MODEL_HDFS_PATH, licenseName));

            List<Tuple2<Integer, List<Integer>>> productRecommendList = productRecommend(model, ALS_RELETIVE_RANK, ALS_BATCH_SIZE);

            Map<Integer, String> idVideo = idVideoPairRDD.collectAsMap();


            ArrayList<Tuple2<String, String>> relativeList = new ArrayList<>();
            for (Tuple2<Integer, List<Integer>> a : productRecommendList) {
                String res = a._2.toString()
                        .replaceAll("\\]", " ")
                        .replaceAll("\\[", " ")
                        .replaceAll(" ", "");
                relativeList.add(new Tuple2<>(String.valueOf(a._1), res));
            }
            JavaRDD<Tuple2<String, String>> relativeRDD = jsc.parallelize(relativeList);
            JavaPairRDD<String, String> stringStringJavaPairRDD = relativeRDD.mapToPair(x -> {
                String[] videoSplit = x._2.split(",");
                String res = "";
                for (int i = 0; i < videoSplit.length; i++) {
                    String transVideo = idVideo.get(Integer.valueOf(videoSplit[i]));
                    if (i == 0) {
                        res = transVideo;
                    } else {
                        res = res + "," + transVideo;
                    }
                }
                String video_id = licenseName + "_" + idVideo.get(Integer.valueOf(x._1));
                return new Tuple2<>(video_id, res);
            });
            JavaRDD<Row> relativeRow = stringStringJavaPairRDD.map(x -> RowFactory.create(x._1, x._2,String.valueOf(ALS_DATA_TIME)));
            ArrayList<StructField> finalRelativeStructFields = new ArrayList<>();
            finalRelativeStructFields.add(DataTypes.createStructField("video_id", DataTypes.StringType, true));
            finalRelativeStructFields.add(DataTypes.createStructField("result_list", DataTypes.StringType, true));
            finalRelativeStructFields.add(DataTypes.createStructField("date_time", DataTypes.StringType, true));
            StructType finalRelativeStructType = DataTypes.createStructType(finalRelativeStructFields);
            Dataset<Row> finalPayDF = ss.createDataFrame(relativeRow, finalRelativeStructType);
            finalPayDF.createOrReplaceTempView("final_relative");
            ss.sql("set hive.exec.dynamic.partition=true");
            ss.sql("set hive.exec.dynamic.partition.mode=nonstrict");
            ss.sql("insert overwrite table knowyou_ott.t_relative PARTITION(date_time) select * from final_relative ");

        }
        jsc.close();
        ss.close();

    }


    /**
     * 对节目进行相关推荐，推荐结果为一个List，其中的元组为(主节目,[推荐节目1,推荐节目2,...])，其中推荐节目按推荐度排序
     *
     * @param model     训练好的模型
     * @param rank      模型中的潜因子数量
     * @param batchSize 每个分块的大小
     * @return
     */
    private static List<Tuple2<Integer, List<Integer>>> productRecommend(MatrixFactorizationModel model, int rank, int batchSize) {
        // 获取productFeatures节目特征列表
        List<Tuple2<Object, double[]>> productFeaturesList = model.productFeatures().toJavaRDD().collect();
        int productSize = productFeaturesList.size();

        // 建立节目特征矩阵，行数为特征数量rank，列数为节目数量productSize
        double[] fullFactors = new double[productSize * rank];
        // 建立节目code索引列表
        int[] code = new int[productSize];
        for (int col = 0; col < productSize; col++) {
            // 获取第col个节目对应的节目code
            code[col] = Integer.valueOf(productFeaturesList.get(col)._1.toString());
            // 获取第col个节目对应的特征数组
            double[] factors = productFeaturesList.get(col)._2;
            // 计算特征向量的长度magnitude
            double normL2 = 0;
            for (int row = 0; row < rank; row++) {
                normL2 += factors[row] * factors[row];
            }
            normL2 = Math.sqrt(normL2);
            // 对节目特征矩阵的第col列赋值
            for (int row = 0; row < rank; row++) {
                fullFactors[col * rank + row] = factors[row] / normL2;
            }
        }

        // 建立节目特征矩阵featureMatrix
        DenseMatrix featureMatrix = new DenseMatrix(rank, productSize, fullFactors);
        System.out.println("featureMatrix cols = " + featureMatrix.numCols() + " rows = " + featureMatrix.numRows());

        // 建立推荐结果列表result
        List<Tuple2<Integer, List<Integer>>> result = new ArrayList<>();

        // 计算节目之间的相似度矩阵similarityMatrix
        // 此处如果节目数量过大，直接做multiply运算，内存会爆，需要分成几个batch来处理
        int batchNum = (int) Math.ceil((double) productSize / batchSize);
        int tailSize = productSize % batchSize;
        System.out.println("batchSize = " + batchSize + " batchNum = " + batchNum);
        for (int batchIndex = 0; batchIndex < batchNum; batchIndex++) {
            int actualBatchSize = batchSize;
            if (tailSize > 0 && batchIndex == batchNum - 1) {
                actualBatchSize = tailSize;
            }


            int batchLength = actualBatchSize * rank;
            double[] batchFactors = new double[batchLength];
            for (int i = 0; i < batchLength; i++) {
                batchFactors[i] = fullFactors[batchIndex * batchSize * rank + i];
            }
            // 建立分块节目特征矩阵batchFeatureMatrix
            DenseMatrix batchFeatureMatrix = new DenseMatrix(rank, actualBatchSize, batchFactors);
            // 计算分块相似度矩阵similarityMatrix
            DenseMatrix similarityMatrix = batchFeatureMatrix.transpose().multiply(featureMatrix);
            System.out.println("batch " + batchIndex + " similarityMatrix cols = " + similarityMatrix.numCols() + " rows = " + similarityMatrix.numRows());

            // 对batch中的每个详情页节目进行TopN相关推荐
            for (int i = 0; i < actualBatchSize; i++) {
                // 详情页节目code
                int acode = code[batchIndex * batchSize + i];
                // 建立(推荐节目code,相似度)元组列表
                List<Tuple2<Integer, Double>> ratings = new ArrayList<>();
                for (int j = 0; j < productSize; j++) {
                    ratings.add(new Tuple2<>(code[j], similarityMatrix.apply(i, j)));
                }
                // 对(推荐节目code,相似度)元组列表进行排序，排序依据为相似度
                List<Tuple2<Integer, Double>> sortedRatings = ratings.stream()
//                        .sorted((v1, v2) -> (int)(v1._2 - v2._2))
                        .sorted(Comparator.comparing(Tuple2::_2))
                        .collect(Collectors.toList());

                // 建立TopN推荐列表
                List<Integer> topNList = new ArrayList<>();
                // 将相似度最高的前TOP_N个节目添加进推荐列表，排除自己
                int order = 0;
                while (topNList.size() < ALS_REC_NUM && order < productSize) {
                    int bcode = sortedRatings.get(productSize - 1 - order)._1;
                    if (bcode == acode) {
                        order++;
                        continue;
                    }
                    topNList.add(bcode);
                    order++;
                }

                // 将(详情页节目code,TopN推荐节目列表)元组添加进推荐结果列表
                result.add(new Tuple2<>(acode, topNList));
            }
        }

        return result;
    }
}
