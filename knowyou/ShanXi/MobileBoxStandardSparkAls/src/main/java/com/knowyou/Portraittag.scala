package com.knowyou

//import org.apache.spark.sql.{Dataset, Row, SparkSession,DataFrame}
//import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.hbase.{HBaseConfiguration,HColumnDescriptor,HTableDescriptor,TableName}
import org.apache.hadoop.hbase.client._
import util.HbaseUtil

object Portraittag {
  def main(args: Array[String]): Unit = {

    val config = util.Config.getInstance()
    val start_date = util.DateUtil.getTwoweeks
    val end_date = util.DateUtil.getYesterday
    val als_database = config.getProperty(util.UtilConstants.AlsRec.ALS_DATABASE)

    //观看频率
    val watch_frequency = "select concat(license,'_',device_id) as rowkey,sum(watch_duration) as tj_watch_frequency from %s.dwd_portrait_playdetail_dtr where dt>='%s' and dt<='%s' group by device_id,license"
    //切换频率
    val switch_frequency = "select concat(license,'_',device_id) as rowkey,count(distinct video_name) as tj_switch_frequency from %s.dwd_portrait_playdetail_dtr where dt>='%s' and dt<='%s' group by device_id,license"
    //活跃度
    val active_level = "select concat(license,'_',device_id) as rowkey,count(distinct dt) as tj_active_level from %s.dwd_portrait_playdetail_dtr where dt>='%s' and dt<='%s' group by device_id,license"
    //活跃时段
    val active_time = "select concat(license,'_',c.deviceid) as rowkey, max(case c.ht when '00' then c.playduration else 0 end) as activetime0, max(case c.ht when '01' then c.playduration else 0 end) as activetime1, max(case c.ht when '02' then c.playduration else 0 end) as activetime2, max(case c.ht when '03' then c.playduration else 0 end) as activetime3, max(case c.ht when '04' then c.playduration else 0 end) as activetime4, max(case c.ht when '05' then c.playduration else 0 end) as activetime5, max(case c.ht when '06' then c.playduration else 0 end) as activetime6, max(case c.ht when '07' then c.playduration else 0 end) as activetime7, max(case c.ht when '08' then c.playduration else 0 end) as activetime8, max(case c.ht when '09' then c.playduration else 0 end) as activetime9, max(case c.ht when '10' then c.playduration else 0 end) as activetime10, max(case c.ht when '11' then c.playduration else 0 end) as activetime11, max(case c.ht when '12' then c.playduration else 0 end) as activetime12, max(case c.ht when '13' then c.playduration else 0 end) as activetime13, max(case c.ht when '14' then c.playduration else 0 end) as activetime14, max(case c.ht when '15' then c.playduration else 0 end) as activetime15, max(case c.ht when '16' then c.playduration else 0 end) as activetime16, max(case c.ht when '17' then c.playduration else 0 end) as activetime17, max(case c.ht when '18' then c.playduration else 0 end) as activetime18, max(case c.ht when '19' then c.playduration else 0 end) as activetime19, max(case c.ht when '20' then c.playduration else 0 end) as activetime20, max(case c.ht when '21' then c.playduration else 0 end) as activetime21, max(case c.ht when '22' then c.playduration else 0 end) as activetime22, max(case c.ht when '23' then c.playduration else 0 end) as activetime23 from( select a.deviceid ,a.ht,a.license,sum(a.playtime)/'%s' as playduration from ( select device_id as deviceid , ht ,license,watch_duration as playtime from %s.dwd_portrait_playdetail_dtr where create_time is not null  and device_id != '' and  dt>='%s' and dt<='%s' )a group by a.deviceid,a.ht,a.license )c group by c.deviceid,c.license"
    //频道分布
    val channel_distribution = "select concat(license,'_',device_id) as rowkey,max(case b.video_type when '1' then b.playduration else 0 end) as live,max(case b.video_type when '2' then b.playduration else 0 end) as dianbo,max(case b.video_type when '3' then b.playduration else 0 end) as huikan from (select a.device_id,a.video_type,a.license,sum(a.watch_duration) as playduration from %s.dwd_portrait_playdetail_dtr a where dt>='%s' and dt<='%s' group by a.device_id,a.video_type,a.license)b group by device_id,license"
    //直播排行
    val live_rank = "select concat(c.license,'_',c.rowkey) as  rowkey, max(case c.rank when '1' then c.video_name else 0 end) as live_top1, max(case c.rank when '2' then c.video_name else 0 end) as live_top2, max(case c.rank when '3' then c.video_name else 0 end) as live_top3, max(case c.rank when '4' then c.video_name else 0 end) as live_top4, max(case c.rank when '5' then c.video_name else 0 end) as live_top5, max(case c.rank when '6' then c.video_name else 0 end) as live_top6, max(case c.rank when '7' then c.video_name else 0 end) as live_top7, max(case c.rank when '8' then c.video_name else 0 end) as live_top8, max(case c.rank when '9' then c.video_name else 0 end) as live_top9, max(case c.rank when '10' then c.video_name else 0 end) as live_top10, max(case c.rank when '1' then c.playnum else 0 end) as live_playnum1, max(case c.rank when '2' then c.playnum else 0 end) as live_playnum2, max(case c.rank when '3' then c.playnum else 0 end) as live_playnum3, max(case c.rank when '4' then c.playnum else 0 end) as live_playnum4, max(case c.rank when '5' then c.playnum else 0 end) as live_playnum5, max(case c.rank when '6' then c.playnum else 0 end) as live_playnum6, max(case c.rank when '7' then c.playnum else 0 end) as live_playnum7, max(case c.rank when '8' then c.playnum else 0 end) as live_playnum8, max(case c.rank when '9' then c.playnum else 0 end) as live_playnum9, max(case c.rank when '10' then c.playnum else 0 end) as live_playnum10 from( select b.rowkey,b.video_name,b.playnum,b.license,row_number() over(partition by b.rowkey order by b.playnum desc)rank from( select a.deviceid as rowkey,a.video_name,a.license,sum(a.watch_duration) as playnum from ( select device_id as deviceid , video_name,license, watch_duration from %s.dwd_portrait_playdetail_dtr where create_time is not null  and device_id != '' and video_name is not null and video_name!='' and video_type='1' and  dt>='%s' and dt<='%s' )a group by a.deviceid,a.video_name,a.license )b )c where c.rank<=10 group by c.rowkey,c.license"
    //点播频道
    val dianbo_rank = "select concat(license,'_',c.deviceid) as rowkey, max(case c.secondlevel when '电影' then c.playduration else 0 end) as film, max(case c.secondlevel when '电视剧' then c.playduration else 0 end) as tv, max(case c.secondlevel when '少儿' then c.playduration else 0 end) as children, max(case c.secondlevel when '综艺' then c.playduration else 0 end) as variety, max(case c.secondlevel when '体育' then c.playduration else 0 end) as sports, max(case c.secondlevel when '纪实' then c.playduration else 0 end) as record, max(case c.secondlevel when '动漫' then c.playduration else 0 end) as entertainment, max(case c.secondlevel when '生活' then c.playduration else 0 end) as life, max(case c.secondlevel when '教育' then c.playduration else 0 end) as edu from( select a.deviceid,a.license ,a.secondlevel,sum(a.playtime) as playduration from ( select deviceid , license,secondlevel,playtime from( select aa.deviceid,bb.secondlevel,aa.license,aa.playtime from( select device_id as deviceid,license,watch_duration as playtime,video_name from %s.dwd_portrait_playdetail_dtr where create_time is not null and dt>='%s' and dt<='%s' and device_id != ''  )aa left join ( select video_name,content_type as secondlevel from %s.dwd_portrait_videoinfo_dtr where dt>='%s' and dt<='%s' )bb on aa.video_name=bb.video_name )aaa where aaa.secondlevel in ('电影','电视剧','少儿','综艺','体育','纪实','动漫','生活','教育') )a group by a.deviceid,a.secondlevel,a.license )c group by c.deviceid,c.license"
    //节目排行
    val content_rank = "select concat(license,'_',c.device_id) as rowkey, max(case c.rank when '1' then c.videoname else 0 end) as video_top1, max(case c.rank when '2' then c.videoname else 0 end) as video_top2, max(case c.rank when '3' then c.videoname else 0 end) as video_top3, max(case c.rank when '4' then c.videoname else 0 end) as video_top4, max(case c.rank when '5' then c.videoname else 0 end) as video_top5, max(case c.rank when '6' then c.videoname else 0 end) as video_top6, max(case c.rank when '7' then c.videoname else 0 end) as video_top7, max(case c.rank when '8' then c.videoname else 0 end) as video_top8, max(case c.rank when '9' then c.videoname else 0 end) as video_top9, max(case c.rank when '10' then c.videoname else 0 end) as video_top10, max(case c.rank when '1' then c.playnum else 0 end) as video_playnum1, max(case c.rank when '2' then c.playnum else 0 end) as video_playnum2, max(case c.rank when '3' then c.playnum else 0 end) as video_playnum3, max(case c.rank when '4' then c.playnum else 0 end) as video_playnum4, max(case c.rank when '5' then c.playnum else 0 end) as video_playnum5, max(case c.rank when '6' then c.playnum else 0 end) as video_playnum6, max(case c.rank when '7' then c.playnum else 0 end) as video_playnum7, max(case c.rank when '8' then c.playnum else 0 end) as video_playnum8, max(case c.rank when '9' then c.playnum else 0 end) as video_playnum9, max(case c.rank when '10' then c.playnum else 0 end) as video_playnum10 from( select b.device_id,b.videoname,b.playnum,b.license,row_number() over(partition by b.device_id order by b.playnum desc)rank from( select a.device_id ,a.videoname,a.license,sum(a.watch_duration)/'%s' as playnum from ( select device_id ,license, watch_duration , video_name as videoname from %s.dwd_portrait_playdetail_dtr where create_time is not null and video_type='2' and device_id != '' and dt>='%s' and dt<='%s' )a group by a.device_id,a.videoname,a.license )b )c where c.rank<=10 group by c.device_id,c.license"

    //内容类型
    val content_type = "select concat(b.license,'_',b.device_id) as rowkey, b.content_type from (select a.*,row_number() over(partition by device_id order by playtime desc)rank from (select device_id,content_type,license,sum(watch_duration) as playtime from %s.dwd_portrait_playdetail_dtr where dt>='%s' and dt<='%s' group by device_id,content_type,license)a)b where b.rank=1"
    //时段分布
    val time_distribution = "select concat(gg.license,'_',gg.device_id) as rowkey,ht from (select ff.*,row_number() over (partition by device_id order by shichang desc)rank from (select a.device_id,a.ht,a.license,sum(watch_duration) as shichang from (select device_id,ht,license,watch_duration from %s.dwd_portrait_playdetail_dtr where dt>='%s' and dt<='%s')a group by a.device_id,a.ht,a.license)ff)gg where gg.rank=1"
    //情节分类
    val plot_type = "select concat(h.license,'_',h.device_id) as device_id,split(h.plot,',')[0] as plot1,split(h.plot,',')[1] as plot2,split(h.plot,',')[2] as plot3,split(h.plot,',')[3] as plot4,split(h.plot,',')[4] as plot5 from (select device_id,license,concat_ws(',',collect_list(g.plot)) as plot from (select device_id,license,plot from (select f.*,row_number() over(partition by device_id order by cishu desc) rank from (select dd.device_id,dd.plot,dd.license,count(*) as cishu from (select c.device_id,c.license,plot from (select b.device_id,b.video_name,b.license,a.video_plot from %s.dwd_portrait_playdetail_dtr b left join (select video_name,video_plot from %s.dwd_portrait_videoinfo_dtr where dt>='%s' and dt<='%s')a on b.video_name = a.video_name where b.dt>='%s' and dt<='%s' and video_plot is not null)c lateral view explode(split(c.video_plot,'\\\\\\\\|')) demo as plot)dd group by dd.device_id,dd.plot,dd.license)f)h where h.rank <=5 )g group by device_id,license)h"
    //融合表
    val fuse = "select bb.rowkey,bb.content_type,bb.ht,bb.plot1,bb.plot2,bb.plot3,bb.plot4,bb.plot5 from((select c.rowkey,c.content_type,t.ht from content c left join time t on c.rowkey = t.rowkey)aa left join plot p on aa.rowkey = p.device_id)bb"
    //评分表
    val score = "select a.rowkey,(case when content_type in ('电影','体育') then 0.5 else 0 end) as bailin_01,(case when ht in ('19','20','21','22','23','24','01') then 0.2 else 0 end) as bailin_02,(case when plot1 in ('科幻','动作','悬疑','犯罪武侠','战争','恐怖','足球','篮球','赛事','历史','人文','职场','都市','纪实','综艺','旅游') then 0.06 else 0 end) as bailin_03,(case when plot2 in ('科幻','动作','悬疑','犯罪武侠','战争','恐怖','足球','篮球','赛事','历史','人文','职场','都市','纪实','综艺','旅游') then 0.06 else 0 end) as bailin_04,(case when plot3 in ('科幻','动作','悬疑','犯罪武侠','战争','恐怖','足球','篮球','赛事','历史','人文','职场','都市','纪实','综艺','旅游') then 0.06 else 0 end) as bailin_05,(case when plot4 in ('科幻','动作','悬疑','犯罪武侠','战争','恐怖','足球','篮球','赛事','历史','人文','职场','都市','纪实','综艺','旅游') then 0.06 else 0 end) as bailin_06,(case when plot5 in ('科幻','动作','悬疑','犯罪武侠','战争','恐怖','足球','篮球','赛事','历史','人文','职场','都市','纪实','综艺','旅游') then 0.06 else 0 end) as bailin_07,(case when content_type in ('电影','电视剧','综艺') then 0.5 else 0 end) as woman_01,(case when ht in ('19','20','21','22','23','24','01') then 0.2 else 0 end) as woman_02,(case when plot1 in ('古装','都市','时尚','穿越','爱情','喜剧','青春','真人秀','偶像','女性','仙侠','综艺','校园','韩剧','文艺','旅游','音乐') then 0.06 else 0 end) as woman_03,(case when plot2 in ('古装','都市','时尚','穿越','爱情','喜剧','青春','真人秀','偶像','女性','仙侠','综艺','校园','韩剧','文艺','旅游','音乐') then 0.06 else 0 end) as woman_04,(case when plot3 in ('古装','都市','时尚','穿越','爱情','喜剧','青春','真人秀','偶像','女性','仙侠','综艺','校园','韩剧','文艺','旅游','音乐') then 0.06 else 0 end) as woman_05,(case when plot4 in ('古装','都市','时尚','穿越','爱情','喜剧','青春','真人秀','偶像','女性','仙侠','综艺','校园','韩剧','文艺','旅游','音乐') then 0.06 else 0 end) as woman_06,(case when plot5 in ('古装','都市','时尚','穿越','爱情','喜剧','青春','真人秀','偶像','女性','仙侠','综艺','校园','韩剧','文艺','旅游','音乐') then 0.06 else 0 end) as woman_07,(case when content_type in ('电影','电视剧','新闻','纪实') then 0.5 else 0 end) as zhongnian_01,(case when ht in ('13','14','15','16','17','18','19') then 0.5 else 0 end) as zhongnian_02,(case when plot1 in ('军旅','抗战','警匪','战争','猎奇','纪实','历史','文化','探索','新闻') then 0.06 else 0 end) as zhongnian_03,(case when plot2 in ('军旅','抗战','警匪','战争','猎奇','纪实','历史','文化','探索','新闻') then 0.06 else 0 end) as zhongnian_04,(case when plot3 in ('军旅','抗战','警匪','战争','猎奇','纪实','历史','文化','探索','新闻') then 0.06 else 0 end) as zhongnian_05,(case when plot4 in ('军旅','抗战','警匪','战争','猎奇','纪实','历史','文化','探索','新闻') then 0.06 else 0 end) as zhongnian_06,(case when plot5 in ('军旅','抗战','警匪','战争','猎奇','纪实','历史','文化','探索','新闻') then 0.06 else 0 end) as zhongnian_07,(case when content_type in ('电视剧','生活','健康') then 0.5 else 0 end) as laole_01,(case when ht in ('07','08','09','10','11','12','13') then 0.2 else 0 end) as laole_02,(case when plot1 in ('军旅','抗战','战争','家庭','伦理','健康','养生','新闻') then 0.06 else 0 end) as laole_03,(case when plot2 in ('军旅','抗战','战争','家庭','伦理','健康','养生','新闻') then 0.06 else 0 end) as laole_04,(case when plot3 in ('军旅','抗战','战争','家庭','伦理','健康','养生','新闻') then 0.06 else 0 end) as laole_05,(case when plot4 in ('军旅','抗战','战争','家庭','伦理','健康','养生','新闻') then 0.06 else 0 end) as laole_06,(case when plot5 in ('军旅','抗战','战争','家庭','伦理','健康','养生','新闻') then 0.06 else 0 end) as laole_07 from (select * from fuse)a"
    //评分计算
    val score_calculation = "select b.rowkey,(case when bailin_03=0 and bailin_04=0 and bailin_05=0 and bailin_06=0 and bailin_07=0 then 0.7 else bailin_01+bailin_02+bailin_03+bailin_04+bailin_05+bailin_06+bailin_07+0.06 end) as bailin,(case when woman_03=0 and woman_04=0 and woman_05=0 and woman_06=0 and woman_07=0 then 0.7 else woman_01+woman_02+woman_03+woman_04+woman_05+woman_06+woman_07+0.06 end) as woman,(case when zhongnian_03=0 and zhongnian_04=0 and zhongnian_05=0 and zhongnian_06=0 and zhongnian_07=0 then 0.7 else zhongnian_01+zhongnian_02+zhongnian_03+zhongnian_04+zhongnian_05+zhongnian_06+zhongnian_07+0.06 end) as zhongnian,(case when laole_03=0 and laole_04=0 and laole_05=0 and laole_06=0 and laole_07=0 then 0.7 else laole_01+laole_02+laole_03+laole_04+laole_05+laole_06+laole_07+0.06 end) as laole from (select * from scorefour)b"
    //判断结果
    val result = "select b.rowkey,(case when b.content_type='少儿' then '家有萌宝' when b.content_type='教育' then '莘莘学子' when s.bailin>=s.woman and s.bailin>=s.zhongnian and s.bailin>=s.laole then '白领男士' when s.woman>=s.bailin and s.woman>=s.zhongnian and s.woman>=s.laole then '时尚女性' when s.zhongnian>=s.bailin and s.zhongnian>=s.woman and s.zhongnian>=s.laole then '人到中年' when s.laole>=s.bailin and s.laole>=s.woman and s.laole>=s.zhongnian then '老有所乐' end) as family_label from score s left join fuse b on s.rowkey=b.rowkey"

//    val cl = Class.forName("com.knowyou.util.HbaseUtil")
//    val method = cl.getDeclaredMethod("createTable", Class[String],Class[Array[String]])
//    method.setAccessible(true)


    val column_list = Array("info")
    val tableName = "hlwdsyyzsns1:hx_portraittag"
//    method.invoke(cl.newInstance(),tableName,column_list)
    val contentTableName = "hlwdsyyzsns1:hx_content_analysis"
//    method.invoke(cl.newInstance(),tableName,column_list)
    val liveTableName = "hlwdsyyzsns1:hx_live_rank"
//    method.invoke(cl.newInstance(),tableName,column_list)
    val dianboTableName = "hlwdsyyzsns1:hx_dianbo_rank"
//    method.invoke(cl.newInstance(),tableName,column_list)
    val contentLabelTableName = "hlwdsyyzsns1:hx_content_label"
//    method.invoke(cl.newInstance(),tableName,column_list)
    val activeTimeTableName = "hlwdsyyzsns1:hx_activetime_rank"
//    method.invoke(cl.newInstance(),tableName,column_list)

    val sparkSession = SparkSession
      .builder
      .config("spark.sql.shuffle.partitions", 100)
//                  .master("local[*]")
      .appName("Portraittag")
      .enableHiveSupport
      .getOrCreate


    val contentType = sparkSession.sql(String.format(content_type,als_database, start_date, end_date)).cache()
    contentType.createOrReplaceTempView("content")


    val timeDistribution = sparkSession.sql(String.format(time_distribution, als_database,start_date,end_date)).cache()
    timeDistribution.createOrReplaceTempView("time")
//    contentType.unpersist()

//      println(String.format(plot_type,start_date,end_date,start_date,end_date))
    val plotType = sparkSession.sql(String.format(plot_type,als_database,als_database,start_date,end_date,start_date,end_date)).cache()
    plotType.createOrReplaceTempView("plot")

    val fuseTable = sparkSession.sql(String.format(fuse)).cache()
    fuseTable.createOrReplaceTempView("fuse")
//      fuseTable.show(5)

    val scoreFour = sparkSession.sql(String.format(score)).cache()
    scoreFour.createOrReplaceTempView("scorefour")

    val scoreCalculation = sparkSession.sql(String.format(score_calculation)).cache()
    scoreCalculation.createOrReplaceTempView("score")

    val family_label = sparkSession.sql(String.format(result))
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, family_label, tableName, "family_label")

    val tj_watch_frequency = sparkSession.sql(String.format(watch_frequency,als_database, start_date, end_date))
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, tj_watch_frequency, tableName, "tj_watch_frequency")
//    tj_watch_frequency.show(10)

    val tj_switch_frequency = sparkSession.sql(String.format(switch_frequency,als_database, start_date, end_date))
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, tj_switch_frequency, tableName, "tj_switch_frequency")
//    tj_switch_frequency.show(10)

    val tj_active_level = sparkSession.sql(String.format(active_level,als_database, start_date, end_date))
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, tj_active_level, tableName, "tj_active_level")
//    tj_active_level.show(10)

    val content_analysis_sqlDF = sparkSession.sql(String.format(channel_distribution,als_database, start_date, end_date)).cache()
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, content_analysis_sqlDF, contentTableName,"live")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, content_analysis_sqlDF, contentTableName,"dianbo")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, content_analysis_sqlDF, contentTableName,"huikan")
//    content_analysis_sqlDF.show(10)
    content_analysis_sqlDF.unpersist()

    val dianbo_rank_sqlDF = sparkSession.sql(String.format(dianbo_rank,als_database, start_date, end_date, als_database, start_date, end_date)).cache()
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, dianbo_rank_sqlDF, dianboTableName, "film")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, dianbo_rank_sqlDF, dianboTableName, "tv")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, dianbo_rank_sqlDF, dianboTableName, "children")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, dianbo_rank_sqlDF, dianboTableName, "variety")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, dianbo_rank_sqlDF, dianboTableName, "sports")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, dianbo_rank_sqlDF, dianboTableName, "record")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, dianbo_rank_sqlDF, dianboTableName, "entertainment")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, dianbo_rank_sqlDF, dianboTableName, "life")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, dianbo_rank_sqlDF, dianboTableName, "edu")
//    dianbo_rank_sqlDF.show(10)
    dianbo_rank_sqlDF.unpersist()

    val live_rank_sqlDF = sparkSession.sql(String.format(live_rank, als_database, start_date, end_date)).cache()
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, live_rank_sqlDF, liveTableName,"live_top1")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, live_rank_sqlDF, liveTableName,"live_top2")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, live_rank_sqlDF, liveTableName,"live_top3")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, live_rank_sqlDF, liveTableName,"live_top4")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, live_rank_sqlDF, liveTableName,"live_top5")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, live_rank_sqlDF, liveTableName,"live_top6")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, live_rank_sqlDF, liveTableName,"live_top7")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, live_rank_sqlDF, liveTableName,"live_top8")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, live_rank_sqlDF, liveTableName,"live_top9")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, live_rank_sqlDF, liveTableName,"live_top10")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, live_rank_sqlDF, liveTableName,"live_playnum1")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, live_rank_sqlDF, liveTableName,"live_playnum2")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, live_rank_sqlDF, liveTableName,"live_playnum3")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, live_rank_sqlDF, liveTableName,"live_playnum4")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, live_rank_sqlDF, liveTableName,"live_playnum5")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, live_rank_sqlDF, liveTableName,"live_playnum6")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, live_rank_sqlDF, liveTableName,"live_playnum7")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, live_rank_sqlDF, liveTableName,"live_playnum8")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, live_rank_sqlDF, liveTableName,"live_playnum9")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, live_rank_sqlDF, liveTableName,"live_playnum10")
//    live_rank_sqlDF.show(10)
    live_rank_sqlDF.unpersist()

    val content_rank_sqlDF = sparkSession.sql(String.format(content_rank,Double.box(14), als_database, start_date, end_date)).cache()
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, content_rank_sqlDF, contentLabelTableName, "video_top1")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, content_rank_sqlDF, contentLabelTableName, "video_top2")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, content_rank_sqlDF, contentLabelTableName, "video_top3")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, content_rank_sqlDF, contentLabelTableName, "video_top4")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, content_rank_sqlDF, contentLabelTableName, "video_top5")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, content_rank_sqlDF, contentLabelTableName, "video_top6")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, content_rank_sqlDF, contentLabelTableName, "video_top7")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, content_rank_sqlDF, contentLabelTableName, "video_top8")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, content_rank_sqlDF, contentLabelTableName, "video_top9")
    util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, content_rank_sqlDF, contentLabelTableName, "video_top10")
//    content_rank_sqlDF.show(10)
    content_rank_sqlDF.unpersist()

    val activetime_rank_sqlDF = sparkSession.sql(String.format(active_time,Double.box(14),als_database,start_date,end_date)).cache()
    var a =0
    for (a<-0 until 24){
      val columnName="activetime"+a
      util.SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, activetime_rank_sqlDF, activeTimeTableName,columnName)
    }
//    activetime_rank_sqlDF.show(10)

    sparkSession.close()
  }

}
