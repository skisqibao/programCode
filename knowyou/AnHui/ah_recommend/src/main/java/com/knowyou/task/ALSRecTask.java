package com.knowyou.task;

import com.knowyou.Scheduler.ALSDataPretreat;
import com.knowyou.Scheduler.ALSRecTranslate;
import com.knowyou.client.HbaseClient;
import com.knowyou.util.CateName;
import com.knowyou.util.FormatTime;
import com.knowyou.util.FreePayType;
import com.knowyou.util.Property;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.Tuple4;
import scala.reflect.ClassManifestFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;

// 多路召回
public class ALSRecTask {
    private static final Logger LOG = LoggerFactory.getLogger(ALSRecTask.class);

    public static void main(String[] args) {
/*        SparkSession ss = SparkSession.builder()
                .appName("ALSRecTask")
                .enableHiveSupport()
                .getOrCreate();*/

        String currentDay = FormatTime.getCurrentDay();

        SparkSession ss = SparkSession.builder()
                .appName("ALSRecTask")
//                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();
        ss.sparkContext().setLogLevel("WARN");

        List<HashMap<String, String>> cateNameList = CateName.toList();
        List<HashMap<String, String>> freePayTypeList = FreePayType.toList();
        for (HashMap<String, String> cateNameMap : cateNameList) {
            for (HashMap<String, String> freePayTypeMap : freePayTypeList) {
                String cateName = cateNameMap.get("cateName");
                String cateNameCode = cateNameMap.get("cateNameCode");
                String freePayType = freePayTypeMap.get("freePayType");
                String freePaySql = freePayTypeMap.get("sql");

//                System.out.println("1 : " + Instant.now());
//                System.out.println(cateName + "\t" + freePayType);

                Broadcast<String> broadcastCateName = ss.sparkContext().broadcast(cateName,
                        ClassManifestFactory.classType(String.class));
                Broadcast<String> broadcastCateCode = ss.sparkContext().broadcast(cateNameCode,
                        ClassManifestFactory.classType(String.class));
                Broadcast<String> broadcastTypeCode = ss.sparkContext().broadcast(freePayType,
                        ClassManifestFactory.classType(String.class));
                ALSDataPretreat alsData = new ALSDataPretreat(cateName, freePaySql);
                Map<String, Object> trainDataMap = alsData.getTrainData(ss);
                JavaRDD<Rating> trainDataRdd = (JavaRDD<Rating>) trainDataMap.get("trainDataRDD");
                JavaPairRDD<String, Long> stringLongVideoPairRdd = (JavaPairRDD<String, Long>) trainDataMap.get(
                        "stringLongVideoPairRDD");
                JavaPairRDD<String, Long> stringLongUserPairRdd = (JavaPairRDD<String, Long>) trainDataMap.get(
                        "stringLongUserPairRDD");
                int disVideoCount = Integer.parseInt(String.valueOf(trainDataMap.get("disVideoCount")));
                if (disVideoCount >= 50) {
                    disVideoCount = 50;
                }

                long trainNumber = trainDataRdd.count();
//                System.out.println("input size：" + trainNumber);
                if (trainNumber == 0) {
                    continue;
                }

                JavaRDD<Rating>[] splitTrainRdd = trainDataRdd.randomSplit(new double[]{0.8, 0.2});
                RDD<Rating> trainRatingRdd = splitTrainRdd[0].rdd();

                int[] ranks = {10};
                int[] iterations = {10};
                double[] lambdas = {10};
                double[] alphas = {60};
                for (int rank : ranks) {
                    for (int iteration : iterations) {
                        for (double lambda : lambdas) {
                            for (double alpha : alphas) {
                                Instant start = Instant.now();

                                MatrixFactorizationModel model = ALS.trainImplicit(trainDataRdd.rdd(), rank, iteration,
                                        lambda, alpha);

                                Instant mid = Instant.now();

                                // 将ALS推荐的Top50节目id列表转换为4个字段：用户自增id，节目自增id，分数，推荐排名
                                JavaRDD<Tuple4<Integer, Integer, Double, Integer>> userRecRdd =
                                        model.recommendProductsForUsers(disVideoCount)
                                                .toJavaRDD()
                                                .mapToPair(x -> new Tuple2<>(x._1, x._2))
                                                .flatMap(x -> {
                                                    Rating[] ratings = x._2;
                                                    List<Tuple4<Integer, Integer, Double, Integer>> records =
                                                            new ArrayList<>();
                                                    for (int i = 0; i < ratings.length; i++) {
                                                        records.add(new Tuple4<>(Integer.parseInt(x._1.toString()),
                                                                ratings[i].product(), ratings[i].rating(), i + 1));
                                                    }
                                                    return records.iterator();
                                                });

                                Instant end = Instant.now();
                                long lt = Duration.between(start, mid).toMillis();
                                long lp = Duration.between(mid, end).toMillis();

//                                System.out.println("start --> " + start);
//                                System.out.println("mid   --> " + mid);
//                                System.out.println("end   --> " + end);
//                                System.out.println("model train   time: " + lt);
//                                System.out.println("model predict time: " + lp);


                                //推荐结果翻译
                                ALSRecTranslate alsRecTranslate = new ALSRecTranslate();
                                Dataset<Row> userDataFrame = alsRecTranslate.getTranslate(ss, userRecRdd,
                                        stringLongUserPairRdd, stringLongVideoPairRdd);
                                userDataFrame.createOrReplaceTempView("rec_result");

                                // Write Hive
                                Dataset<Row> rowDataset = ss.sql(String.format("select deviceid, videoid, rating, " +
                                                "rank, " +
                                                "'%s' AS videotype, '%s' AS paytype, '%s' AS dt from rec_result",
                                        broadcastCateName.value(), broadcastTypeCode.value(), currentDay));
//                                rowDataset.show();

//                                System.out.println("output size : " + rowDataset.count());
                                System.out.println(freePayType + "--" + cateName + "\t" + trainNumber + "\t" + lt +
                                        "\t" + lp + "\t" + rowDataset.count());

                                rowDataset.write()
                                        .mode("append")
                                        .partitionBy("dt")
                                        .saveAsTable("knowyou_ott_dmt.guess_like_all_type_recall_res");

                                // Write HBase
/*                                Dataset<Row> alsRecResultDf = ss.sql("SELECT o.deviceid, \n" +
                                        "concat( '[', concat_ws(',', collect_list(json)), ']' ) AS rec \n" +
                                        "FROM ( SELECT n1.deviceid, \n" +
                                        "concat( '{\\\"cpid\\\":\\\"\\\",\\\"itemid\\\":\\\"', COALESCE (n1.itemid, " +
                                        "''), '\\\",\\\"itemname\\\":\\\"', COALESCE (n1.itemname, ''), '\\\"," +
                                        "\\\"weight\\\":', 1, ',\\\"picurl\\\":\\\"', COALESCE (n1.picurl, ''), " +
                                        "'\\\",\\\"apkurl\\\":\\\"\\\",\\\"cls\\\":\\\"\\\",\\\"pkg\\\":\\\"\\\"," +
                                        "\\\"cornerurl\\\":\\\"\\\",\\\"backtype\\\":\\\"home\\\"," +
                                        "\\\"cornerposition\\\":\\\"3\\\",\\\"param1\\\":\\\"\\\"," +
                                        "\\\"param2\\\":\\\"',\n" +
                                        "COALESCE (n1.param2, ''),'\\\",\\\"param3\\\":\\\"',COALESCE (n1.param3, '')" +
                                        ",'\\\"}' ) AS json \n" +
                                        "from(\n" +
                                        "SELECT t1.deviceid, t1.videoid as itemid, t1.rating, t1.rank, t2.series_name" +
                                        " as itemname, \n" +
                                        "t2.picture_series_1_1_fileurl as picurl, t2.series_cmsid as param2, t2" +
                                        ".package_series as param3\n" +
                                        "FROM  rec_result t1 \n" +
                                        "LEFT JOIN( \n" +
                                        "SELECT series,series_name, picture_series_1_1_fileurl ,series_cmsid, " +
                                        "package_series\n" +
                                        "FROM knowyou_ott_ods.dim_pub_video_df\n" +
                                        "where dt in (select max(dt) as dt from knowyou_ott_ods.dim_pub_video_df)\n" +
                                        ")t2 \n" +
                                        "ON t1.videoid=t2.series \n" +
                                        ")n1\n" +
                                        ")o GROUP BY o.deviceid ");
                                JavaRDD<Row> alsRecResultRdd = alsRecResultDf.toJavaRDD();
                                alsRecResultRdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
                                    @Override
                                    public void call(Iterator<Row> rowIterator) throws Exception {
                                        while (rowIterator.hasNext()) {
                                            Row row = rowIterator.next();
                                            String deviceId = row.getString(0);
                                            try {
                                                String rowkey =
                                                        deviceId.substring(deviceId.length() - 12) + "_" +
                                                        broadcastCateCode.value() + "_" + broadcastTypeCode.value();
                                                String dataValue = row.getString(1);
                                                HbaseClient.putData("rec_guess", rowkey, "info", "items", dataValue);
                                            } catch (Exception e) {
                                                LOG.error("deviceid 长度不足12，数据为：" + deviceId);
                                            }
                                        }
                                    }
                                });
*/
                            }
                        }
                    }
                }
            }
        }
        ss.close();
    }
}
