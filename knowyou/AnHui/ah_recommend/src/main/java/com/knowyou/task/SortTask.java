package com.knowyou.task;

import com.knowyou.client.HbaseClient;
import com.knowyou.util.CateName;
import com.knowyou.util.FormatTime;
import com.knowyou.util.FreePayType;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.reflect.ClassManifestFactory;

import java.util.*;

public class SortTask {
    private static final Logger LOG = LoggerFactory.getLogger(ALSRecTask.class);

    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder()
                .appName("sort step")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();
        ss.sparkContext().setLogLevel("WARN");

        String currentDay = FormatTime.getCurrentDay();
        List<HashMap<String, String>> cateNameList = CateName.toList();
        List<HashMap<String, String>> freePayTypeList = FreePayType.toList();
        for (HashMap<String, String> cateNameMap : cateNameList) {
            for (HashMap<String, String> freePayTypeMap : freePayTypeList) {
                String cateName = cateNameMap.get("cateName");
                String cateNameCode = cateNameMap.get("cateNameCode");
                String freePayType = freePayTypeMap.get("freePayType");
                Broadcast<String> broadcastCateName = ss.sparkContext().broadcast(cateName,
                        ClassManifestFactory.classType(String.class));
                Broadcast<String> broadcastTypeCode = ss.sparkContext().broadcast(freePayType,
                        ClassManifestFactory.classType(String.class));
                Broadcast<String> broadcastCateCode = ss.sparkContext().broadcast(cateNameCode,
                        ClassManifestFactory.classType(String.class));

                // 读取ALS召回
                currentDay = "20220712";
                String alsSQL = String.format("select " +
                                "deviceid, videoid from knowyou_ott_dmt.guess_like_all_type_recall_res " +
                                "where videotype = '%s' and paytype = '%s' and dt = '%s' ",
                        broadcastCateName.value(),
                        broadcastTypeCode.value(), currentDay);
                Dataset<Row> alsCallDF = ss.sql(alsSQL);
                System.out.println("ALS推荐：" + alsSQL);

                // 读取热门召回
                String hotSQL = String.format("select videoid from  knowyou_ott_dmt.guess_like_all_type_hot_res " +
                                "where videotype = '%s' and paytype = '%s' and dt = '%s' ",
                        broadcastCateName.value(),
                        broadcastTypeCode.value(), currentDay);
                Dataset<Row> hotCallDF = ss.sql(hotSQL);
                System.out.println("热门推荐：" + hotSQL);

                // 所有路线推荐节目合并去重清单
                JavaPairRDD<String, String> hotDeviceidCallRDD = alsCallDF.javaRDD()
                        .map(row -> {
                            return row.getString(0);
                        })
                        .randomSplit(new double[]{0.4, 0.6})[0]
                        .distinct()
                        .cartesian(hotCallDF.toJavaRDD().map(row -> {
                            return row.getString(0);
                        }));

                System.out.println("热门推荐总数：" + hotDeviceidCallRDD.count());

                JavaRDD<Row> allRecVideoRDD = alsCallDF.javaRDD()
                        .mapToPair(row -> {
                            String deviceid = row.getString(0);
                            String videoid = row.getString(1);
                            return new Tuple2<>(deviceid, videoid);
                        }).union(hotDeviceidCallRDD)
                        .distinct()
                        .map(x -> RowFactory.create(String.valueOf(x._1), String.valueOf(x._2)));

                System.out.println("ALS+热门推荐总数：" + allRecVideoRDD.count());

                List<StructField> structFields = new ArrayList<>();
                structFields.add(DataTypes.createStructField("deviceid", DataTypes.StringType, true));
                structFields.add(DataTypes.createStructField("videoid", DataTypes.StringType, true));
                StructType recStructType = DataTypes.createStructType(structFields);
                Dataset<Row> allRecVideoDF = ss.createDataFrame(allRecVideoRDD, recStructType);
                allRecVideoDF.createOrReplaceTempView("rec_result");

                // 关联推荐节目媒资属性和短期用户兴趣标签并计算各类别得分
                String selectSQL = "" +
                        "select deviceid,videoid,sum(free_rate) free_rate, sum(type_rate) type_rate, sum(regin_rate) " +
                        "regin_rate, sum(language_rate) language_rate, sum" +
                        "(year_rate) year_rate, sum(plot_rate) plot_rate \n" +
                        "from\n" +
                        "(" +
                        "  select a.deviceid, a.videoid,\n" +
                        "    case when b.tag = '1' and b.type_data = c.is_free then b.play_rate else 0 end free_rate," +
                        "\n" +
                        "    case when b.tag = '2' and b.type_data = c.content_type then b.play_rate else 0 end " +
                        "type_rate,\n" +
                        "    case when b.tag = '3' and array_contains(split(c.regin, '\\\\|'), b.type_data) then b" +
                        ".play_rate / size(split(c.regin, '\\\\|')) else 0 end regin_rate,\n" +
                        "    case when b.tag = '4' and array_contains(split(c.language, '\\\\|'), b.type_data) then b" +
                        ".play_rate / size(split(c.language, '\\\\|')) else 0 end language_rate,\n" +
                        "    case when b.tag = '5' and b.type_data = c.year then b.play_rate else 0 end year_rate,\n" +
                        "    case when b.tag = '6' and array_contains(split(c.plot, '\\\\|'), b.type_data) then b" +
                        ".play_rate / size(split(c.plot, '\\\\|')) else 0 end plot_rate\n" +
                        "  from rec_result a\n" +
                        "  left join\n" +
                        "  (\n";

                String userSQL = "";
                if ("all".equalsIgnoreCase(broadcastCateName.value())) {
                    userSQL = String.format("" +
                                    "    select a.deviceid, tag, type_data, case when tag = '6' then round" +
                                    "(playnum/total, 4) else" +
                                    "  play_rate end play_rate from\n" +
                                    "    (\n" +
                                    "      select deviceid, tag, type_data, playnum, play_rate \n" +
                                    "      from knowyou_ott_dmt.guess_like_user_interest_preference_rate\n" +
                                    "      where dt = '%s' and index_classify = '1' \n" +
                                    "    ) a\n" +
                                    "    left join \n" +
                                    "    (\n" +
                                    "      select deviceid, sum(playnum) total from knowyou_ott_dmt" +
                                    "  .guess_like_user_interest_preference_rate\n" +
                                    "      where dt = '%s' and index_classify = '1' and tag = '6' group by deviceid\n" +
                                    "    ) b on a.deviceid = b.deviceid\n",
                            currentDay, currentDay);
                } else {
                    userSQL = String.format("" +
                                    "    select deviceid, tag, type_data, play_rate \n" +
                                    "    from knowyou_ott_dmt.guess_like_user_interest_preference_rate\n" +
                                    "    where dt = '%s' and index_classify = '1'\n",
                            currentDay);
                }
                String mediaSQL = String.format("" +
                                "  ) b on a.deviceid = b.deviceid\n" +
                                "  left join\n" +
                                "  (\n" +
                                "    select video_id, year, is_free, content_type, regin, language, plot \n" +
                                "    from knowyou_ott_dmt.guess_like_media_attribute \n" +
                                "    where dt = '%s'\n" +
                                "  ) c on a.videoid = c.video_id \n" +
                                ") t\n" +
                                "group by deviceid, videoid",
                        currentDay);

                String ratingSQL = selectSQL + userSQL + mediaSQL;
                System.out.println(ratingSQL);
                Dataset<Row> ratingDS = ss.sql(ratingSQL);

                ratingDS.show(false);

                ratingDS.selectExpr("deviceid", "videoid",
                        "CAST(CAST((free_rate + type_rate + regin_rate + language_rate + year_rate + plot_rate) AS " +
                                "double) AS DECIMAL(18, 2)) AS rating")
                        .show(false);

                Dataset<Row> recommendDS = ratingDS.selectExpr("deviceid", "videoid",
                        "CAST(CAST((0*free_rate + 0*type_rate + regin_rate + language_rate + year_rate + 5*plot_rate)" +
                                " AS double) AS DECIMAL(18, 2)) AS raing");

                recommendDS.show(false);

                recommendDS.createOrReplaceTempView("recommend_list");

                // Write HBase
                String recSQL = "" +
                        "SELECT \n" +
                        "  o.deviceid, \n" +
                        "  concat( '[', concat_ws(',', collect_list(json)), ']' ) AS rec \n" +
                        "FROM \n" +
                        "( \n" +
                        "  SELECT \n" +
                        "    n1.deviceid, \n" +
                        "    concat( \n" +
                        "      '{\\\\\\\"cpid\\\\\\\":\\\\\\\"\\\\\\\",\n" +
                        "        \\\\\\\"itemid\\\\\\\":\\\\\\\"', COALESCE (n1.itemid, ''), '\\\\\\\",\n" +
                        "        \\\\\\\"itemname\\\\\\\":\\\\\\\"', COALESCE (n1.itemname, ''), '\\\\\\\",\n" +
                        "        \\\\\\\"weight\\\\\\\":', 1, ',\n" +
                        "        \\\\\\\"picurl\\\\\\\":\\\\\\\"', COALESCE (n1.picurl, ''), '\\\\\\\",\n" +
                        "        \\\\\\\"apkurl\\\\\\\":\\\\\\\"\\\\\\\",\n" +
                        "        \\\\\\\"cls\\\\\\\":\\\\\\\"\\\\\\\",\n" +
                        "        \\\\\\\"pkg\\\\\\\":\\\\\\\"\\\\\\\",\n" +
                        "        \\\\\\\"cornerurl\\\\\\\":\\\\\\\"\\\\\\\",\n" +
                        "        \\\\\\\"backtype\\\\\\\":\\\\\\\"home\\\\\\\",\n" +
                        "        \\\\\\\"cornerposition\\\\\\\":\\\\\\\"3\\\\\\\",\n" +
                        "        \\\\\\\"param1\\\\\\\":\\\\\\\"\\\\\\\",\n" +
                        "        \\\\\\\"param2\\\\\\\":\\\\\\\"',COALESCE (n1.param2, ''),'\\\\\\\",\n" +
                        "        \\\\\\\"param3\\\\\\\":\\\\\\\"',COALESCE (n1.param3, ''),'\\\\\\\"}' \n" +
                        "    ) AS json \n" +
                        "  FROM\n" +
                        "  (\n" +
                        "    SELECT \n" +
                        "      t1.deviceid, \n" +
                        "      t1.videoid as itemid, \n" +
                        "      t1.rating, \n" +
                        "      t1.rank, \n" +
                        "      t2.series_name as itemname, \n" +
                        "      t2.picture_series_1_1_fileurl as picurl, \n" +
                        "      t2.series_cmsid as param2, t2.package_series as param3\n" +
                        "    FROM \n" +
                        "    (\n" +
                        "      SELECT deviceid, videoid, rating, rank FROM\n" +
                        "      (\n" +
                        "        SELECT \n" +
                        "          deviceid, videoid, rating, \n" +
                        "          ROW_NUMBER() OVER(PARTITION BY deviceid ORDER BY rating DESC) as rank\n" +
                        "        FROM recommend_list\n" +
                        "      )\n" +
                        "      WHERE rank <= 100\n" +
                        "    ) t1 \n" +
                        "    LEFT JOIN\n" +
                        "    ( \n" +
                        "        SELECT series,series_name, picture_series_1_1_fileurl ,series_cmsid, " +
                        "package_series\n" +
                        "        FROM knowyou_ott_ods.dim_pub_video_df\n" +
                        "        where dt in (select max(dt) as dt from knowyou_ott_ods.dim_pub_video_df)\n" +
                        "    )t2 ON t1.videoid=t2.series \n" +
                        "  )n1\n" +
                        ")o GROUP BY o.deviceid ";
                Dataset<Row> alsRecResultDf = ss.sql(recSQL);
                JavaRDD<Row> alsRecResultRdd = alsRecResultDf.toJavaRDD();
                alsRecResultRdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
                    @Override
                    public void call(Iterator<Row> rowIterator) throws Exception {
                        while (rowIterator.hasNext()) {
                            Row row = rowIterator.next();
                            String deviceId = row.getString(0);
                            try {
                                String rowkey =
                                        deviceId.substring(deviceId.length() - 12) + "_" + broadcastCateCode.value() + "_" + broadcastTypeCode.value();
                                String dataValue = row.getString(1);
                                HbaseClient.putData("new_rec_guess", rowkey, "info", "items",
                                        dataValue);
                            } catch (Exception e) {
                                LOG.error("deviceid 长度不足12，数据为：" + deviceId);
                            }
                        }
                    }
                });

            }
        }

        ss.stop();
    }
}
