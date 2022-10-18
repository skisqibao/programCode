package com.knowyou.task;

import com.knowyou.client.HbaseClient;
import com.knowyou.util.CateName;
import com.knowyou.util.FormatTime;
import com.knowyou.util.FreePayType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.reflect.ClassManifestFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * @author Knowyou
 */
public class HotRecTask {

    private static final Logger LOG = LoggerFactory.getLogger(HotRecTask.class);

    public static void main(String[] args) {
        SparkSession ss = SparkSession.builder()
                .appName("HotRecTask")
                .master("local[*]")
                .enableHiveSupport()
                .getOrCreate();

        ss.sparkContext().setLogLevel("WARN");
        String currentDay = FormatTime.getCurrentDay();
        String sevenDayBefore = FormatTime.fTime(currentDay, 7);
        String sqlBase = String.format("select  '' as deviceid,b.seriesheadcode as videoid,'' as rating,b.rank " +
                "from( select a.seriesheadcode,a.series_type, " +
                "row_number () over( partition by a.series_type ORDER BY a.playnum DESC) AS rank " +
                "from ( select seriesheadcode, series_type ,count(*) as playnum " +
                "from knowyou_ott_ods.dws_rec_video_playinfo_di where dt>='%s' and dt <='%s' ", sevenDayBefore, currentDay);
        String sqlEnd = "group by seriesheadcode,series_type )a )b where b.rank<=100";
        String allSqlEnd = "group by seriesheadcode,series_type )a )b where b.rank<=10";
        List<HashMap<String, String>> cateNameList = CateName.toList();
        List<HashMap<String, String>> freePayTypeList = FreePayType.toList();
        for (HashMap<String, String> cateNameMap : cateNameList) {
            for (HashMap<String, String> freePayTypeMap : freePayTypeList) {
                String cateName = cateNameMap.get("cateName");
                String cateNameCode = cateNameMap.get("cateNameCode");
                String freePayType = freePayTypeMap.get("freePayType");
                String freePaySql = freePayTypeMap.get("sql");
                Broadcast<String> broadcastCateCode = ss.sparkContext().broadcast(cateNameCode, ClassManifestFactory.classType(String.class));
                Broadcast<String> broadcastCateName = ss.sparkContext().broadcast(cateName, ClassManifestFactory.classType(String.class));
                Broadcast<String> broadcastTypeCode = ss.sparkContext().broadcast(freePayType, ClassManifestFactory.classType(String.class));
                String dataSource = "";
                if ("all".equalsIgnoreCase(cateName)) {
                    dataSource = String.format(sqlBase + " %s " + allSqlEnd, freePaySql);
                } else {
                    dataSource = String.format(sqlBase + " and series_type='%s' %s " + sqlEnd, cateName, freePaySql);
                }
                System.out.println(dataSource);
                Dataset<Row> dataSourceDf = ss.sql(dataSource);
                dataSourceDf.createOrReplaceTempView("rec_result");

                // Write Hive
                Dataset<Row> rowDataset = ss.sql(String.format("select videoid, rank, " +
                                "'%s' AS videotype, '%s' AS paytype, '%s' AS dt from rec_result",
                        broadcastCateName.value(), broadcastTypeCode.value(), currentDay));
                rowDataset.show();
                rowDataset.write()
                        .mode("append")
                        .partitionBy("dt")
                        .saveAsTable("knowyou_ott_dmt.guess_like_all_type_hot_res");

                // Write Hbase
/*                Dataset<Row> hotRecResultDf = ss.sql("SELECT o.deviceid, \n" +
                        "concat( '[', concat_ws(',', collect_list(json)), ']' ) AS rec \n" +
                        "FROM ( SELECT n1.deviceid, \n" +
                        "concat( '{\\\"cpid\\\":\\\"\\\",\\\"itemid\\\":\\\"', COALESCE (n1.itemid, ''), '\\\",\\\"itemname\\\":\\\"', COALESCE (n1.itemname, ''), '\\\",\\\"weight\\\":', 1, ',\\\"picurl\\\":\\\"', COALESCE (n1.picurl, ''), '\\\",\\\"apkurl\\\":\\\"\\\",\\\"cls\\\":\\\"\\\",\\\"pkg\\\":\\\"\\\",\\\"cornerurl\\\":\\\"\\\",\\\"backtype\\\":\\\"home\\\",\\\"cornerposition\\\":\\\"3\\\",\\\"param1\\\":\\\"\\\",\\\"param2\\\":\\\"',\n" +
                        "COALESCE (n1.param2, ''),'\\\",\\\"param3\\\":\\\"',COALESCE (n1.param3, ''),'\\\"}' ) AS json \n" +
                        "from(\n" +
                        "SELECT t1.deviceid, t1.videoid as itemid, t1.rating, t1.rank, t2.series_name as itemname, \n" +
                        "t2.picture_series_1_1_fileurl as picurl, t2.series_cmsid as param2, t2.package_series as param3\n" +
                        "FROM  rec_result t1 \n" +
                        "LEFT JOIN( \n" +
                        "SELECT series,series_name, picture_series_1_1_fileurl ,series_cmsid, package_series\n" +
                        "FROM knowyou_ott_ods.dim_pub_video_df\n" +
                        "where dt in (select max(dt) as dt from knowyou_ott_ods.dim_pub_video_df)\n" +
                        ")t2 \n" +
                        "ON t1.videoid=t2.series \n" +
                        ")n1\n" +
                        ")o GROUP BY o.deviceid ");
                JavaRDD<Row> hotRecResultRdd = hotRecResultDf.toJavaRDD();
                hotRecResultRdd.foreachPartition(new VoidFunction<Iterator<Row>>() {
                    @Override
                    public void call(Iterator<Row> rowIterator) throws Exception {
                        while (rowIterator.hasNext()) {
                            String cateCode = broadcastCateCode.value();
                            String typeCode = broadcastTypeCode.value();
                            Row row = rowIterator.next();
                            String rowkey = cateCode + "_" + typeCode;
                            String dataValue = row.getString(1);
                            HbaseClient.putData("rec_hot", rowkey, "info", "items", dataValue);
                        }
                    }
                });
*/
            }
        }
        ss.close();
    }
}
