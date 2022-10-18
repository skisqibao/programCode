package com.knowyou.Scheduler;

import com.knowyou.map.*;
import com.knowyou.util.FormatTime;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

/**
 * 基于ALS模型推荐所需数据格式
 */
public class ALSDataPretreat {

    private static final String DIST_DEVIEID_SQL = "select distinct deviceid from data_source";
    private static final String DIST_VIDEOID_SQL = "select distinct seriesheadcode from data_source";
    String dataSource = "";

    public ALSDataPretreat(String cateName, String freePaySql) {

        String currentDay = FormatTime.getCurrentDay();
        String sevenDayBefore = FormatTime.fTime(currentDay, 7);

        String sqlBase = String.format("select deviceid, seriesheadcode, series_type as catalog " +
                "from knowyou_ott_ods.dws_rec_video_playinfo_di " +
                "where  dt>='%s' and dt <='%s' ", sevenDayBefore, currentDay);

        /*if ("all".equalsIgnoreCase(cateName)) {
            dataSource = String.format(sqlBase + " %s ", freePaySql);
        } else {
            dataSource = String.format(sqlBase + " and series_type ='%s' %s ", cateName, freePaySql);
        }*/

        // 在原有基础上添加评分项
        String changeSql = "";
        if ("all".equalsIgnoreCase(cateName)) {
            changeSql = String.format("" +
                            "select \n" +
                            "  deviceid, seriesheadcode, catalog, \n" +
                            "  cast(case when rating > 1 then 1 else rating end as double) rating\n" +
                            "from\n" +
                            "(\n" +
                            "  select \n" +
                            "    deviceid, t1.seriesheadcode seriesheadcode, catalog, \n" +
                            "    cast(cast(sum_time/cen_time as double) as decimal(18,2)) rating\n" +
                            "  from\n" +
                            "  (\n" +
                            "    select \n" +
                            "      deviceid, seriesheadcode, series_type as catalog, \n" +
                            "      sum(cast(playtime as int)) as sum_time \n" +
                            "    from \n" +
                            "    (\n" +
                            "      SELECT t1.deviceid deviceid, t1.seriesheadcode seriesheadcode, series_type, " +
                            "playtime FROM\n" +
                            "        (\n" +
                            "            SELECT deviceid, seriesheadcode, series_type, playtime\n" +
                            "            FROM knowyou_ott_ods.dws_rec_video_playinfo_di \n" +
                            "            WHERE dt BETWEEN '%s' AND '%s' and length(deviceid)>0 and length" +
                            "(seriesheadcode)>0 and deviceid is not null and seriesheadcode is not null and playtime " +
                            "is not null %s\n" +
                            "            GROUP BY deviceid, seriesheadcode, series_type, playtime\n" +
                            "        ) t1 \n" +
                            "        JOIN (\n" +
                            "            SELECT seriesheadcode FROM\n" +
                            "            (\n" +
                            "                SELECT seriesheadcode, count(distinct deviceid) cnt FROM knowyou_ott_ods" +
                            ".dws_rec_video_playinfo_di  \n" +
                            "                WHERE dt BETWEEN '%s' AND '%s'\n" +
                            "                GROUP BY seriesheadcode\n" +
                            "            ) a WHERE cnt > 2\n" +
                            "        ) t2\n" +
                            "        ON t1.seriesheadcode = t2.seriesheadcode\n" +
                            "        JOIN (\n" +
                            "            SELECT deviceid, count(seriesheadcode) FROM knowyou_ott_ods" +
                            ".dws_rec_video_playinfo_di\n" +
                            "            WHERE dt BETWEEN '%s' AND '%s'\n" +
                            "            GROUP BY deviceid\n" +
                            "            HAVING count(seriesheadcode) > 2     \n" +
                            "        ) t3\n" +
                            "        ON t1.deviceid = t3.deviceid\n" +
                            "    ) a \n" +
                            "    group by deviceid, seriesheadcode, series_type    \n" +
                            "  ) t1\n" +
                            "  left join\n" +
                            "  (\n" +
                            "    select \n" +
                            "      seriesheadcode, percentile(cast(playtime as int),0.5) as cen_time \n" +
                            "    from knowyou_ott_ods.dws_rec_video_playinfo_di \n" +
                            "    where dt between '%s' and '%s' and playtime is not null %s\n" +
                            "    group by seriesheadcode\n" +
                            "  ) t2 \n" +
                            "  on t1.seriesheadcode = t2.seriesheadcode\n" +
                            ") t where rating > 0",
                    sevenDayBefore, currentDay, freePaySql, sevenDayBefore, currentDay, sevenDayBefore, currentDay,
                    sevenDayBefore, currentDay,
                    freePaySql);
        } else {
            changeSql = String.format("" +
                            "select \n" +
                            "  deviceid, seriesheadcode, catalog, \n" +
                            "  cast(case when rating > 1 then 1 else rating end as double) rating\n" +
                            "from\n" +
                            "(\n" +
                            "  select \n" +
                            "    deviceid, t1.seriesheadcode seriesheadcode, catalog, \n" +
                            "    cast(cast(sum_time/cen_time as double) as decimal(18,2)) rating\n" +
                            "  from\n" +
                            "  (\n" +
                            "    select \n" +
                            "      deviceid, seriesheadcode, series_type as catalog, \n" +
                            "      sum(cast(playtime as int)) as sum_time \n" +
                            "    from \n" +
                            "    (\n" +
                            "      SELECT t1.deviceid deviceid, t1.seriesheadcode seriesheadcode, series_type, " +
                            "playtime FROM\n" +
                            "        (\n" +
                            "            SELECT deviceid, seriesheadcode, series_type, playtime\n" +
                            "            FROM knowyou_ott_ods.dws_rec_video_playinfo_di \n" +
                            "            WHERE dt BETWEEN '%s' AND '%s' and length(deviceid)>0 and length" +
                            "(seriesheadcode)>0 and deviceid is not null and seriesheadcode is not null and playtime " +
                            "is not null and series_type " +
                            "='%s' %s \n" +
                            "            GROUP BY deviceid, seriesheadcode, series_type, playtime\n" +
                            "        ) t1 \n" +
                            "        JOIN (\n" +
                            "            SELECT seriesheadcode FROM\n" +
                            "            (\n" +
                            "                SELECT seriesheadcode, count(distinct deviceid) cnt FROM knowyou_ott_ods" +
                            ".dws_rec_video_playinfo_di  \n" +
                            "                WHERE dt BETWEEN '%s' AND '%s'\n" +
                            "                GROUP BY seriesheadcode\n" +
                            "            ) a WHERE cnt > 2\n" +
                            "        ) t2\n" +
                            "        ON t1.seriesheadcode = t2.seriesheadcode\n" +
                            "        JOIN (\n" +
                            "            SELECT deviceid, count(seriesheadcode) FROM knowyou_ott_ods" +
                            ".dws_rec_video_playinfo_di\n" +
                            "            WHERE dt BETWEEN '%s' AND '%s'\n" +
                            "            GROUP BY deviceid\n" +
                            "            HAVING count(seriesheadcode) > 2    \n" +
                            "        ) t3\n" +
                            "        ON t1.deviceid = t3.deviceid\n" +
                            "    ) a \n" +
                            "    group by deviceid, seriesheadcode, series_type \n" +
                            "  ) t1\n" +
                            "  left join\n" +
                            "  (\n" +
                            "    select \n" +
                            "      seriesheadcode, percentile(cast(playtime as int),0.5) as cen_time \n" +
                            "    from knowyou_ott_ods.dws_rec_video_playinfo_di \n" +
                            "    where dt between '%s' and '%s' and playtime is not null and series_type ='%s' %s \n" +
                            "    group by seriesheadcode\n" +
                            "  ) t2 \n" +
                            "  on t1.seriesheadcode = t2.seriesheadcode\n" +
                            ") t where rating > 0",
                    sevenDayBefore, currentDay, cateName, freePaySql,
                    sevenDayBefore, currentDay, sevenDayBefore, currentDay,
                    sevenDayBefore, currentDay, cateName, freePaySql);
        }

        dataSource = changeSql;

    }

    /**
     * 得到分隔数据集
     *
     * @param sparkSession sparksession
     * @return
     */
    public Map<String, Object> getTrainData(SparkSession sparkSession) {
        Map<String, Object> map = new HashMap<>();
        Dataset<Row> dataSourceDf = sparkSession.sql(dataSource);
        dataSourceDf.createOrReplaceTempView("data_source");

//        System.out.println(dataSource + "，类型数据个数：" + dataSourceDf.count());

        Dataset<Row> distDeviceDs = sparkSession.sql(DIST_DEVIEID_SQL);
        JavaPairRDD<String, Long> stringLongUserPairRdd = distDeviceDs.toJavaRDD()
                .map(new RowFirstFunc()).zipWithIndex();
        Dataset<Row> distVideoDd = sparkSession.sql(DIST_VIDEOID_SQL);
        long disVideoCount = distVideoDd.count();
        JavaPairRDD<String, Long> stringLongVideoPairRdd = distVideoDd.toJavaRDD()
                .map(new RowFirstFunc()).zipWithIndex();

        JavaRDD<Rating> trainDataRdd = dataSourceDf.toJavaRDD()
                .mapToPair(new RowToDeviceTupleFunc())
                .join(stringLongUserPairRdd)
                .mapToPair(new DevicePairJoinFunc())
                .join(stringLongVideoPairRdd)
                .mapToPair(new VideoPairJoinFunc())
                .map(new PairToRatingFunc());
        map.put("stringLongUserPairRDD", stringLongUserPairRdd);
        map.put("stringLongVideoPairRDD", stringLongVideoPairRdd);
        map.put("trainDataRDD", trainDataRdd);
        map.put("disVideoCount", disVideoCount);

        return map;
    }
}
