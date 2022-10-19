CREATE EXTERNAL TABLE `ods_mz_relative_content_map`(
`seriesheadcode` string COMMENT '???? ', 
`updatetype` string COMMENT '???? ', 
`cpid` string COMMENT '??????? ', 
`catalog` string COMMENT '???? ', 
`seriesname` string COMMENT '???? ', 
`contentcode` string COMMENT '???? ', 
`contentname` string COMMENT '???? ', 
`seriesnum` string COMMENT '??????? ', 
`videotype` string COMMENT '???? ', 
`elapsedtime` string COMMENT '?? ', 
`resolution` string COMMENT '??? ', 
`genre` string COMMENT '???? ', 
`createtime` string COMMENT '???? ', 
`description` string COMMENT '???? ', 
`directors` string COMMENT '?? ', 
`actors` string COMMENT '?? ', 
`orgairdate` string COMMENT '???? ', 
`country` string COMMENT '???? ', 
`language` string COMMENT '?? ', 
`score` string COMMENT '???? ', 
`movieid` string COMMENT 'movieid')
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'  
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,info:updatetype,info:cpid,info:catalog,info:seriesname,info:contentcode,info:contentname,info:seriesnum,info:videotype,info:elapsedtime,info:resolution,info:genre,info:createtime,info:description,info:directors,info:actors,info:orgairdate,info:country,info:language,info:score,info:movieid")
TBLPROPERTIES("hbase.table.name" = "relative_content");

----module----------------------------------------------------------------------

CREATE EXTERNAL TABLE `hive_hbase_table_mapping.table`(
    `` STRING,
    `` STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,
    info:column1,
    info:column2")
TBLPROPERTIES("hbase.table.name" = "name");

----相关推荐----------------------------------------------------------------------

CREATE EXTERNAL TABLE `hive_hbase_table_mapping.relative`(
    `video_id` STRING,
    `rec_list` STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,info:recommend")
TBLPROPERTIES("hbase.table.name" = "relative");

----猜你喜欢----------------------------------------------------------------------

CREATE EXTERNAL TABLE `hive_hbase_table_mapping.rec_guess_32`(
    `device_id` STRING,
    `rec_list` STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,info:recommend")
TBLPROPERTIES("hbase.table.name" = "rec_guess_32");

CREATE EXTERNAL TABLE `hive_hbase_table_mapping.rec_guess_3`(
    `device_id` STRING,
    `rec_list` STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,info:recommend")
TBLPROPERTIES("hbase.table.name" = "rec_guess_3");

----各时段活跃度------------------------------------------------------------------

CREATE EXTERNAL TABLE `hive_hbase_table_mapping.hx_activetime_rank`(
    device_id       STRING,
    activetime0     STRING,
    activetime1     STRING,
    activetime10    STRING,
    activetime11    STRING,
    activetime12    STRING,
    activetime13    STRING,
    activetime14    STRING,
    activetime15    STRING,
    activetime16    STRING,
    activetime17    STRING,
    activetime18    STRING,
    activetime19    STRING,
    activetime2     STRING,
    activetime20    STRING,
    activetime21    STRING,
    activetime22    STRING,
    activetime23    STRING,
    activetime3     STRING,
    activetime4     STRING,
    activetime5     STRING,
    activetime6     STRING,
    activetime7     STRING,
    activetime8     STRING,
    activetime9     STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,
    info:activetime0, 
    info:activetime1, 
    info:activetime10,
    info:activetime11,
    info:activetime12,
    info:activetime13,
    info:activetime14,
    info:activetime15,
    info:activetime16,
    info:activetime17,
    info:activetime18,
    info:activetime19,
    info:activetime2, 
    info:activetime20,
    info:activetime21,
    info:activetime22,
    info:activetime23,
    info:activetime3, 
    info:activetime4, 
    info:activetime5, 
    info:activetime6, 
    info:activetime7, 
    info:activetime8, 
    info:activetime9")
TBLPROPERTIES("hbase.table.name" = "hx_activetime_rank");

----内容分析----------------------------------------------------------------------
                                                                                                                                  
CREATE EXTERNAL TABLE `hive_hbase_table_mapping.hx_content_analysis`(
    device_id    STRING,
    dianbo       STRING,
    huikan       STRING,
    live         STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,
    info:dianbo,
    info:huikan,
    info:live")
TBLPROPERTIES("hbase.table.name" = "hx_content_analysis");

----内容标签----------------------------------------------------------------------

CREATE EXTERNAL TABLE `hive_hbase_table_mapping.hx_content_label`(
    device_id       STRING,
    video_top1      STRING,
    video_top10     STRING,
    video_top2      STRING,
    video_top3      STRING,
    video_top4      STRING,
    video_top5      STRING,
    video_top6      STRING,
    video_top7      STRING,
    video_top8      STRING,
    video_top9      STRING
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,
    info:video_top1,      
    info:video_top10,
    info:video_top2,      
    info:video_top3, 
    info:video_top4, 
    info:video_top5, 
    info:video_top6, 
    info:video_top7, 
    info:video_top8, 
    info:video_top9")
TBLPROPERTIES("hbase.table.name" = "hx_content_label");

----点播排行----------------------------------------------------------------------

CREATE EXTERNAL TABLE `hive_hbase_table_mapping.hx_dianbo_rank`(
    device_id        STRING,
    children         STRING,
    edu              STRING,
    entertainment    STRING,
    film             STRING,
    life             STRING,
    record           STRING,
    sports           STRING,
    tv               STRING,
    variety          STRING 
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,
    info:children,
    info:edu,
    info:entertainment,
    info:film, 
    info:life, 
    info:record, 
    info:sports, 
    info:tv, 
    info:variety ")
TBLPROPERTIES("hbase.table.name" = "hx_dianbo_rank");

----直播排行----------------------------------------------------------------------

CREATE EXTERNAL TABLE `hive_hbase_table_mapping.hx_live_rank`(
    device_id         STRING,
    live_playnum1     STRING,
    live_playnum10    STRING,
    live_playnum2     STRING,
    live_playnum3     STRING,
    live_playnum4     STRING,
    live_playnum5     STRING,
    live_playnum6     STRING,
    live_playnum7     STRING,
    live_playnum8     STRING,
    live_playnum9     STRING,
    live_top1         STRING,
    live_top10        STRING,
    live_top2         STRING,
    live_top3         STRING,
    live_top4         STRING,
    live_top5         STRING,
    live_top6         STRING,
    live_top7         STRING,
    live_top8         STRING,
    live_top9         STRING 
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,
    info:live_playnum1, 
    info:live_playnum10,
    info:live_playnum2, 
    info:live_playnum3, 
    info:live_playnum4, 
    info:live_playnum5, 
    info:live_playnum6, 
    info:live_playnum7, 
    info:live_playnum8, 
    info:live_playnum9, 
    info:live_top1, 
    info:live_top10, 
    info:live_top2, 
    info:live_top3, 
    info:live_top4, 
    info:live_top5, 
    info:live_top6, 
    info:live_top7, 
    info:live_top8, 
    info:live_top9 ")
TBLPROPERTIES("hbase.table.name" = "hx_live_rank");

----画像标签----------------------------------------------------------------------

CREATE EXTERNAL TABLE `hive_hbase_table_mapping.hx_portraittag`(
    device_id              STRING,
    family_label           STRING,
    tj_active_level        STRING,
    tj_switch_frequency    STRING,
    tj_watch_frequency     STRING 
)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
    "hbase.columns.mapping" = ":key,
    info:family_label,
    info:tj_active_level,
    info:tj_switch_frequency,
    info:tj_watch_frequency ")
TBLPROPERTIES("hbase.table.name" = "hx_portraittag");

------------------------------------------------------------------------------
#!/bin/bash

database=hive_hbase_table_mapping

tables+=(hx_activetime_rank)
tables+=(hx_content_analysis)
tables+=(hx_content_label)
tables+=(hx_dianbo_rank)
tables+=(hx_live_rank)
tables+=(hx_portraittag)
tables+=(rec_guess_32)
tables+=(relative)

for table in ${tables[@]}
do
    hive -e "SELECT * FROM ${database}.${table} LIMIT 1000" >> ./${database}/${table}.bcp
done




select 
    t1.device_id,
    family_label,
    concat_ws('#', collect_list(video_id))
from
(
    select 
        device_id, concat(a.video_id, '--', b.content_type) video_id
    FROM
    (
        select 
            device_id, 
            get_json_object(rec_json, '$.video_id') video_id,
            get_json_object(rec_json, '$.order') rec_order
        FROM hive_hbase_table_mapping.rec_guess_3
        lateral view explode(split(regexp_replace(regexp_replace(rec_list , '\\[|\\]',''), '\\}\\,\\{', '\\}\\;\\{'), '\\;')) tmp as rec_json
        sort by device_id, cast(rec_order as int)
    ) a
    left join
    (
        select albumname video_name, MAX(channelid) content_type from knowyou_ott_ods.ods_newtv_albumsdata_dm 
        where dt=20220831 group by albumname
    ) b
    on a.video_id = b.video_name
) t1
left join
(
    select 
        device_id,
        family_label
    from hive_hbase_table_mapping.hx_portraittag 
) t2
on t1.device_id = t2.device_id
group by t1.device_id, family_label


select 
    device_id, family_label, 
    CASE WHEN content_types = '其他' THEN content_types
    WHEN content_types like '%其他|%' THEN REGEXP_REPLACE(content_types, '其他\\|', '') 
    ELSE REGEXP_REPLACE(content_types, '\\|其他', '')END content_types
from
(
    select 
        t1.device_id,
        family_label,
        concat_ws('|', collect_set(NVL(content_type,'其他'))) content_types
    from
    (
        select 
            device_id, video_id,  
            case
            when content_type = '1000001' then '电影' 
            when content_type = '1000002' then '电视剧' 
            when content_type = '1000003' then '少儿' 
            when content_type = '1000004' then '综艺' 
            when content_type = '1000005' then '音乐' 
            when content_type = '1000006' then '体育' 
            when content_type = '1000007' then '教育' 
            when content_type = '1000008' then '片花' 
            when content_type = '1000009' then '纪录片' 
            when content_type = '1000010' then '娱乐' 
            when content_type = '1000011' then '时尚' 
            when content_type = '1000012' then '搞笑' 
            when content_type = '1000013' then '少儿' 
            when content_type = '1000014' then '游戏' 
            when content_type = '1000019' then '新闻' 
            when content_type = '1000020' then '健康' 
            when content_type = '1000021' then '军事' 
            when content_type = '1000022' then '科技' 
            when content_type = '1000023' then '财经' 
            when content_type = '1000024' then '母婴' 
            when content_type = '1000025' then '旅游' 
            when content_type = '1000026' then '汽车' 
            when content_type = '1000027' then '资讯' 
            when content_type = '1000028' then '原创'
            when content_type = '1000029' then '生活'
            when content_type = '1000030' then '少儿'
            end content_type
        FROM
        (
            select 
                device_id, 
                get_json_object(rec_json, '$.video_id') video_id,
                get_json_object(rec_json, '$.order') rec_order
            FROM hive_hbase_table_mapping.rec_guess_3
            lateral view explode(split(regexp_replace(regexp_replace(rec_list , '\\[|\\]',''), '\\}\\,\\{', '\\}\\;\\{'), '\\;')) tmp as rec_json
            sort by device_id, cast(rec_order as int)
        ) a
        join
        (
            select albumname video_name, MAX(channelid) content_type from knowyou_ott_ods.ods_newtv_albumsdata_dm 
            where dt=20220831 group by albumname
        ) b
        on a.video_id = b.video_name
    
        UNION ALL
    
        select 
            device_id, video_id, content_type
        from
        (
            select 
                device_id, video_id
            FROM
            (
                select 
                    device_id, 
                    get_json_object(rec_json, '$.video_id') video_id,
                    get_json_object(rec_json, '$.order') rec_order
                FROM hive_hbase_table_mapping.rec_guess_3
                lateral view explode(split(regexp_replace(regexp_replace(rec_list , '\\[|\\]',''), '\\}\\,\\{', '\\}\\;\\{'), '\\;')) tmp as rec_json
                sort by device_id, cast(rec_order as int)
            ) a
            left join
            (
                select albumname video_name, MAX(channelid) content_type from knowyou_ott_ods.ods_newtv_albumsdata_dm 
                where dt=20220831 group by albumname
            ) b
            on a.video_id = b.video_name
            where b.video_name is null
        ) s
        left join 
        (
            select video_name, MAX(content_type) content_type from knowyou_jituan_dmt.ads_rec_userdetail_wtr 
            where dt=20220831 and length(content_type) > 0 and content_type != 'NULL' and content_type != '其他' group by video_name
        ) k
        on s.video_id = k.video_name
    ) t1 
    left join
    (
        select 
            device_id,
            family_label
        from hive_hbase_table_mapping.hx_portraittag 
    ) t2
    on t1.device_id = t2.device_id
    group by t1.device_id, family_label
) x


        select albumname video_name, channelid content_type from knowyou_ott_ods.ods_newtv_albumsdata_dm 
        where dt=20220831 group by albumname, channelid
        
        select albumname video_name, max(channelid) content_type from knowyou_ott_ods.ods_newtv_albumsdata_dm 
        where dt=20220831 group by albumname

select distinct 
    video_id
from
(
    select 
        device_id, 
        get_json_object(rec_json, '$.video_id') video_id,
        get_json_object(rec_json, '$.order') rec_order
    FROM hive_hbase_table_mapping.rec_guess_3
    lateral view explode(split(regexp_replace(regexp_replace(rec_list , '\\[|\\]',''), '\\}\\,\\{', '\\}\\;\\{'), '\\;')) tmp as rec_json
    sort by device_id, cast(rec_order as int)
) t1
where video_id rlike '^\\d+$'

hive -e "select
 zz.month,
 zz.deviceid,
 qq.userID,
 zz.videotype,
 zz.playTimes,
 zz.state,
 ww.product_name,
 gg.family_label,
 gg.video_id
 from 
      (select
           distinct
           month                                      
          ,deviceid                                   
          ,type as  videotype                                     
          ,playtime as playTimes                                  
          ,tag  as state     
      from default.stbidactive0901) zz
 left join 
     (select
         distinct
         deviceid,
         userID
     from default.stbidcpall) qq
 on zz.deviceid=qq.deviceid
 left join
     (select
         mm.userID,
         mm.product_name,
         nn.deviceid,
         mm.month
     from 
             (select
                 distinct
                 product_name,
                 mobile_number as userID,
                 substr(date_time,1,6) as month
             from knowyou_jituan_edw.jk_list
             where date_time>='20220801' and date_time<='20220831')mm
     left join 
            (select
                distinct
                deviceid,
                userID
            from default.stbidcpall) nn
            on mm.userID=nn.userID)ww
 on zz.deviceid=ww.deviceid and zz. month=ww.month
 left join
       (select device_id, family_label, videos video_id, '点播' as videotype from default.guess_rec_result )gg
on  zz.deviceid= gg.device_id and zz.videotype=gg.videotype
 
 " > /tmp/qushu20220905.txt



