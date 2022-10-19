select o.videoname, x.videoname from
(
  select x.videoname, n.colx from
  (
    select split(video_id, '_')[1] as video_id, colx FROM 
    (
      select video_id, rec_list from hive_hbase_table_mapping.relative DISTRIBUTE BY rand() SORT BY rand() LIMIT 597
    ) m
    lateral view explode(split(rec_list, ',')) tablex AS colx
  )n
  left join
  (
    select key,videoname from knowyou_jituan_edw.edw_ba_cn_videoinformation group by key,videoname
  ) x
  on n.video_id = x.key
) o
left join
(
    select key,videoname from knowyou_jituan_edw.edw_ba_cn_videoinformation group by key,videoname
) x
on o.colx = x.key

-- hx_activetime_rank
SELECT c.deviceid AS rowkey,
       max(CASE c.ht WHEN '00' THEN c.playduration ELSE 0 END) AS activetime0,
       max(CASE c.ht WHEN '01' THEN c.playduration ELSE 0 END) AS activetime1,
       max(CASE c.ht WHEN '02' THEN c.playduration ELSE 0 END) AS activetime2,
       max(CASE c.ht WHEN '03' THEN c.playduration ELSE 0 END) AS activetime3,
       max(CASE c.ht WHEN '04' THEN c.playduration ELSE 0 END) AS activetime4,
       max(CASE c.ht WHEN '05' THEN c.playduration ELSE 0 END) AS activetime5,
       max(CASE c.ht WHEN '06' THEN c.playduration ELSE 0 END) AS activetime6,
       max(CASE c.ht WHEN '07' THEN c.playduration ELSE 0 END) AS activetime7,
       max(CASE c.ht WHEN '08' THEN c.playduration ELSE 0 END) AS activetime8,
       max(CASE c.ht WHEN '09' THEN c.playduration ELSE 0 END) AS activetime9,
       max(CASE c.ht WHEN '10' THEN c.playduration ELSE 0 END) AS activetime10,
       max(CASE c.ht WHEN '11' THEN c.playduration ELSE 0 END) AS activetime11,
       max(CASE c.ht WHEN '12' THEN c.playduration ELSE 0 END) AS activetime12,
       max(CASE c.ht WHEN '13' THEN c.playduration ELSE 0 END) AS activetime13,
       max(CASE c.ht WHEN '14' THEN c.playduration ELSE 0 END) AS activetime14,
       max(CASE c.ht WHEN '15' THEN c.playduration ELSE 0 END) AS activetime15,
       max(CASE c.ht WHEN '16' THEN c.playduration ELSE 0 END) AS activetime16,
       max(CASE c.ht WHEN '17' THEN c.playduration ELSE 0 END) AS activetime17,
       max(CASE c.ht WHEN '18' THEN c.playduration ELSE 0 END) AS activetime18,
       max(CASE c.ht WHEN '19' THEN c.playduration ELSE 0 END) AS activetime19,
       max(CASE c.ht WHEN '20' THEN c.playduration ELSE 0 END) AS activetime20,
       max(CASE c.ht WHEN '21' THEN c.playduration ELSE 0 END) AS activetime21,
       max(CASE c.ht WHEN '22' THEN c.playduration ELSE 0 END) AS activetime22,
       max(CASE c.ht WHEN '23' THEN c.playduration ELSE 0 END) AS activetime23 from
  (SELECT a.deviceid ,a.ht,sum(a.playtime)/'14.0' AS playduration
   FROM
     (SELECT device_id AS deviceid , ht ,watch_duration AS playtime
      FROM knowyou_jituan_dmt.dwd_portrait_playdetail_dtr
      WHERE create_time IS NOT NULL
        AND device_id = '005803FF000323100006A8BD3A9FAC54'
        AND dt>='20210716'
        AND dt<='20210730')a
   GROUP BY a.deviceid,a.ht)c
GROUP BY c.deviceid;

-- hx_content_analysis
SELECT device_id AS rowkey,
       max(CASE b.video_type WHEN '1' THEN b.playduration ELSE 0 END) AS live,
       max(CASE b.video_type WHEN '2' THEN b.playduration ELSE 0 END) AS dianbo,
       max(CASE b.video_type WHEN '3' THEN b.playduration ELSE 0 END) AS huikan
FROM
  (SELECT a.device_id,
          a.video_type,
          sum(a.watch_duration) AS playduration
   FROM knowyou_jituan_dmt.dwd_portrait_playdetail_dtr a
   WHERE dt>='20210716'
     AND dt<='20210730'
     AND device_id = '005803FF000323100006A8BD3A9FAC54'
   GROUP BY a.device_id,
            a.video_type)b
GROUP BY device_id

-- hx_content_label
SELECT c.device_id AS rowkey,
       max(CASE c.rank WHEN '1' THEN c.videoname ELSE 0 END) AS video_top1,
       max(CASE c.rank WHEN '2' THEN c.videoname ELSE 0 END) AS video_top2,
       max(CASE c.rank WHEN '3' THEN c.videoname ELSE 0 END) AS video_top3,
       max(CASE c.rank WHEN '4' THEN c.videoname ELSE 0 END) AS video_top4,
       max(CASE c.rank WHEN '5' THEN c.videoname ELSE 0 END) AS video_top5,
       max(CASE c.rank WHEN '6' THEN c.videoname ELSE 0 END) AS video_top6,
       max(CASE c.rank WHEN '7' THEN c.videoname ELSE 0 END) AS video_top7,
       max(CASE c.rank WHEN '8' THEN c.videoname ELSE 0 END) AS video_top8,
       max(CASE c.rank WHEN '9' THEN c.videoname ELSE 0 END) AS video_top9,
       max(CASE c.rank WHEN '10' THEN c.videoname ELSE 0 END) AS video_top10,
       max(CASE c.rank WHEN '1' THEN c.playnum ELSE 0 END) AS video_playnum1,
       max(CASE c.rank WHEN '2' THEN c.playnum ELSE 0 END) AS video_playnum2,
       max(CASE c.rank WHEN '3' THEN c.playnum ELSE 0 END) AS video_playnum3,
       max(CASE c.rank WHEN '4' THEN c.playnum ELSE 0 END) AS video_playnum4,
       max(CASE c.rank WHEN '5' THEN c.playnum ELSE 0 END) AS video_playnum5,
       max(CASE c.rank WHEN '6' THEN c.playnum ELSE 0 END) AS video_playnum6,
       max(CASE c.rank WHEN '7' THEN c.playnum ELSE 0 END) AS video_playnum7,
       max(CASE c.rank WHEN '8' THEN c.playnum ELSE 0 END) AS video_playnum8,
       max(CASE c.rank WHEN '9' THEN c.playnum ELSE 0 END) AS video_playnum9,
       max(CASE c.rank WHEN '10' THEN c.playnum ELSE 0 END) AS video_playnum10 from
  (SELECT b.device_id,b.videoname,b.playnum,row_number() over(partition BY b.device_id          ORDER BY b.playnum DESC)rank from
     (SELECT a.device_id ,a.videoname,sum(a.watch_duration)/'14.0' AS playnum
      FROM
        (SELECT device_id , watch_duration , video_name AS videoname
         FROM knowyou_jituan_dmt.dwd_portrait_playdetail_dtr
         WHERE create_time IS NOT NULL
           AND video_type='2'
           AND device_id = '005803FF000323100006A8BD3A9FAC54'
           AND dt>='20210716'
           AND dt<='20210730')a
      GROUP BY a.device_id,a.videoname)b)c
WHERE c.rank<=10
GROUP BY c.device_id

-- hx_dianbo_rank
SELECT c.deviceid AS rowkey,
       max(CASE c.secondlevel WHEN '电影' THEN c.playduration ELSE 0 END) AS film,
       max(CASE c.secondlevel WHEN '电视剧' THEN c.playduration ELSE 0 END) AS tv,
       max(CASE c.secondlevel WHEN '少儿' THEN c.playduration ELSE 0 END) AS children,
       max(CASE c.secondlevel WHEN '综艺' THEN c.playduration ELSE 0 END) AS variety,
       max(CASE c.secondlevel WHEN '体育' THEN c.playduration ELSE 0 END) AS sports,
       max(CASE c.secondlevel WHEN '纪实' THEN c.playduration ELSE 0 END) AS record,
       max(CASE c.secondlevel WHEN '动漫' THEN c.playduration ELSE 0 END) AS entertainment,
       max(CASE c.secondlevel WHEN '生活' THEN c.playduration ELSE 0 END) AS life,
       max(CASE c.secondlevel WHEN '教育' THEN c.playduration ELSE 0 END) AS edu from
  (SELECT a.deviceid ,a.secondlevel,sum(a.playtime) AS playduration
   FROM
     (SELECT deviceid , secondlevel,playtime
      FROM
        (SELECT aa.deviceid,bb.secondlevel,aa.playtime
         FROM
           (SELECT device_id AS deviceid,watch_duration AS playtime,video_name
            FROM knowyou_jituan_dmt.dwd_portrait_playdetail_dtr
            WHERE create_time IS NOT NULL
              AND dt>='20210716'
              AND dt<='20210730'
              AND device_id = '005803FF000323100006A8BD3A9FAC54')aa
         LEFT JOIN
           (SELECT video_name,content_type AS secondlevel
            FROM knowyou_jituan_dmt.dwd_portrait_videoinfo_dtr
            WHERE dt>='20210716'
              AND dt<='20210730')bb ON aa.video_name=bb.video_name)aaa
      WHERE aaa.secondlevel IN ('电影','电视剧','少儿','综艺','体育','纪实','动漫','生活','教育'))a
   GROUP BY a.deviceid,a.secondlevel)c
GROUP BY c.deviceid

-- hx_live_rank
SELECT c.rowkey,
       max(CASE c.rank WHEN '1' THEN c.video_name ELSE 0 END) AS live_top1,
       max(CASE c.rank WHEN '2' THEN c.video_name ELSE 0 END) AS live_top2,
       max(CASE c.rank WHEN '3' THEN c.video_name ELSE 0 END) AS live_top3,
       max(CASE c.rank WHEN '4' THEN c.video_name ELSE 0 END) AS live_top4,
       max(CASE c.rank WHEN '5' THEN c.video_name ELSE 0 END) AS live_top5,
       max(CASE c.rank WHEN '6' THEN c.video_name ELSE 0 END) AS live_top6,
       max(CASE c.rank WHEN '7' THEN c.video_name ELSE 0 END) AS live_top7,
       max(CASE c.rank WHEN '8' THEN c.video_name ELSE 0 END) AS live_top8,
       max(CASE c.rank WHEN '9' THEN c.video_name ELSE 0 END) AS live_top9,
       max(CASE c.rank WHEN '10' THEN c.video_name ELSE 0 END) AS live_top10,
       max(CASE c.rank WHEN '1' THEN c.playnum ELSE 0 END) AS live_playnum1,
       max(CASE c.rank WHEN '2' THEN c.playnum ELSE 0 END) AS live_playnum2,
       max(CASE c.rank WHEN '3' THEN c.playnum ELSE 0 END) AS live_playnum3,
       max(CASE c.rank WHEN '4' THEN c.playnum ELSE 0 END) AS live_playnum4,
       max(CASE c.rank WHEN '5' THEN c.playnum ELSE 0 END) AS live_playnum5,
       max(CASE c.rank WHEN '6' THEN c.playnum ELSE 0 END) AS live_playnum6,
       max(CASE c.rank WHEN '7' THEN c.playnum ELSE 0 END) AS live_playnum7,
       max(CASE c.rank WHEN '8' THEN c.playnum ELSE 0 END) AS live_playnum8,
       max(CASE c.rank WHEN '9' THEN c.playnum ELSE 0 END) AS live_playnum9,
       max(CASE c.rank WHEN '10' THEN c.playnum ELSE 0 END) AS live_playnum10 from
  (SELECT b.rowkey,b.video_name,b.playnum,row_number() over(partition BY b.rowkey        ORDER BY b.playnum DESC)rank from
     (SELECT a.deviceid AS rowkey,a.video_name,sum(a.watch_duration) AS playnum
      FROM
        (SELECT device_id AS deviceid , video_name, watch_duration
         FROM knowyou_jituan_dmt.dwd_portrait_playdetail_dtr
         WHERE create_time IS NOT NULL
           AND device_id = '005803FF000323100006A8BD3A9FAC54'
           AND video_name IS NOT NULL
           AND video_name!=''
           AND video_type='1'
           AND dt>='20210716'
           AND dt<='20210730')a
      GROUP BY a.deviceid,a.video_name)b)c
WHERE c.rank<=10
GROUP BY c.rowkey

-- hx_portraittag
SELECT b.rowkey,
       (CASE
            WHEN b.content_type='少儿' THEN '家有萌宝'
            WHEN b.content_type='教育' THEN '莘莘学子'
            WHEN s.bailin>=s.woman
                 AND s.bailin>=s.zhongnian
                 AND s.bailin>=s.laole THEN '白领男士'
            WHEN s.woman>=s.bailin
                 AND s.woman>=s.zhongnian
                 AND s.woman>=s.laole THEN '时尚女性'
            WHEN s.zhongnian>=s.bailin
                 AND s.zhongnian>=s.woman
                 AND s.zhongnian>=s.laole THEN '人到中年'
            WHEN s.laole>=s.bailin
                 AND s.laole>=s.woman
                 AND s.laole>=s.zhongnian THEN '老有所乐'
        END) AS family_label
FROM score s
LEFT JOIN fuse b ON s.rowkey=b.rowkey

            SELECT a.rowkey,
                   (CASE
                        WHEN content_type IN ('电影',    '体育') THEN 0.5
                        ELSE 0
                    END) AS bailin_01,
                   (CASE
                        WHEN ht IN ('19',  '20',  '21',  '22',  '23',  '24',  '01') THEN 0.2
                        ELSE 0
                    END) AS bailin_02,
                   (CASE
                        WHEN plot1 IN ('科幻', '动作', '悬疑', '犯罪武侠', '战争', '恐怖', '足球', '篮球', '赛事', '历史', '人文', '职场', '都市', '纪实', '综艺', '旅游') THEN 0.06
                        ELSE 0
                    END) AS bailin_03,
                   (CASE
                        WHEN plot2 IN ('科幻', '动作', '悬疑', '犯罪武侠', '战争', '恐怖', '足球', '篮球', '赛事', '历史', '人文', '职场', '都市', '纪实', '综艺', '旅游') THEN 0.06
                        ELSE 0
                    END) AS bailin_04,
                   (CASE
                        WHEN plot3 IN ('科幻', '动作', '悬疑', '犯罪武侠', '战争', '恐怖', '足球', '篮球', '赛事', '历史', '人文', '职场', '都市', '纪实', '综艺', '旅游') THEN 0.06
                        ELSE 0
                    END) AS bailin_05,
                   (CASE
                        WHEN plot4 IN ('科幻', '动作', '悬疑', '犯罪武侠', '战争', '恐怖', '足球', '篮球', '赛事', '历史', '人文', '职场', '都市', '纪实', '综艺', '旅游') THEN 0.06
                        ELSE 0
                    END) AS bailin_06,
                   (CASE
                        WHEN plot5 IN ('科幻', '动作', '悬疑', '犯罪武侠', '战争', '恐怖', '足球', '篮球', '赛事', '历史', '人文', '职场', '都市', '纪实', '综艺', '旅游') THEN 0.06
                        ELSE 0
                    END) AS bailin_07,
                   (CASE
                        WHEN content_type IN ('电影',        '电视剧',        '综艺') THEN 0.5
                        ELSE 0
                    END) AS woman_01,
                   (CASE
                        WHEN ht IN ('19',  '20',  '21',  '22',  '23',  '24',  '01') THEN 0.2
                        ELSE 0
                    END) AS woman_02,
                   (CASE
                        WHEN plot1 IN ('古装', '都市', '时尚', '穿越', '爱情', '喜剧', '青春', '真人秀', '偶像', '女性', '仙侠', '综艺', '校园', '韩剧', '文艺', '旅游', '音乐') THEN 0.06
                        ELSE 0
                    END) AS woman_03,
                   (CASE
                        WHEN plot2 IN ('古装', '都市', '时尚', '穿越', '爱情', '喜剧', '青春', '真人秀', '偶像', '女性', '仙侠', '综艺', '校园', '韩剧', '文艺', '旅游', '音乐') THEN 0.06
                        ELSE 0
                    END) AS woman_04,
                   (CASE
                        WHEN plot3 IN ('古装', '都市', '时尚', '穿越', '爱情', '喜剧', '青春', '真人秀', '偶像', '女性', '仙侠', '综艺', '校园', '韩剧', '文艺', '旅游', '音乐') THEN 0.06
                        ELSE 0
                    END) AS woman_05,
                   (CASE
                        WHEN plot4 IN ('古装', '都市', '时尚', '穿越', '爱情', '喜剧', '青春', '真人秀', '偶像', '女性', '仙侠', '综艺', '校园', '韩剧', '文艺', '旅游', '音乐') THEN 0.06
                        ELSE 0
                    END) AS woman_06,
                   (CASE
                        WHEN plot5 IN ('古装', '都市', '时尚', '穿越', '爱情', '喜剧', '青春', '真人秀', '偶像', '女性', '仙侠', '综艺', '校园', '韩剧', '文艺', '旅游', '音乐') THEN 0.06
                        ELSE 0
                    END) AS woman_07,
                   (CASE
                        WHEN content_type IN ('电影',        '电视剧',        '新闻',        '纪实') THEN 0.5
                        ELSE 0
                    END) AS zhongnian_01,
                   (CASE
                        WHEN ht IN ('13', '14', '15', '16', '17', '18', '19') THEN 0.5
                        ELSE 0
                    END) AS zhongnian_02,
                   (CASE
                        WHEN plot1 IN ('军旅', '抗战', '警匪', '战争', '猎奇', '纪实', '历史', '文化', '探索', '新闻') THEN 0.06
                        ELSE 0
                    END) AS zhongnian_03,
                   (CASE
                        WHEN plot2 IN ('军旅', '抗战', '警匪', '战争', '猎奇', '纪实', '历史', '文化', '探索', '新闻') THEN 0.06
                        ELSE 0
                    END) AS zhongnian_04,
                   (CASE
                        WHEN plot3 IN ('军旅', '抗战', '警匪', '战争', '猎奇', '纪实', '历史', '文化', '探索', '新闻') THEN 0.06
                        ELSE 0
                    END) AS zhongnian_05,
                   (CASE
                        WHEN plot4 IN ('军旅', '抗战', '警匪', '战争', '猎奇', '纪实', '历史', '文化', '探索', '新闻') THEN 0.06
                        ELSE 0
                    END) AS zhongnian_06,
                   (CASE
                        WHEN plot5 IN ('军旅', '抗战', '警匪', '战争', '猎奇', '纪实', '历史', '文化', '探索', '新闻') THEN 0.06
                        ELSE 0
                    END) AS zhongnian_07,
                   (CASE
                        WHEN content_type IN ('电视剧',  '生活',  '健康') THEN 0.5
                        ELSE 0
                    END) AS laole_01,
                   (CASE
                        WHEN ht IN ('07', '08', '09', '10', '11', '12', '13') THEN 0.2
                        ELSE 0
                    END) AS laole_02,
                   (CASE
                        WHEN plot1 IN ('军旅', '抗战', '战争', '家庭', '伦理', '健康', '养生', '新闻') THEN 0.06
                        ELSE 0
                    END) AS laole_03,
                   (CASE
                        WHEN plot2 IN ('军旅', '抗战', '战争', '家庭', '伦理', '健康', '养生', '新闻') THEN 0.06
                        ELSE 0
                    END) AS laole_04,
                   (CASE
                        WHEN plot3 IN ('军旅', '抗战', '战争', '家庭', '伦理', '健康', '养生', '新闻') THEN 0.06
                        ELSE 0
                    END) AS laole_05,
                   (CASE
                        WHEN plot4 IN ('军旅', '抗战', '战争', '家庭', '伦理', '健康', '养生', '新闻') THEN 0.06
                        ELSE 0
                    END) AS laole_06,
                   (CASE
                        WHEN plot5 IN ('军旅', '抗战', '战争', '家庭', '伦理', '健康', '养生', '新闻') THEN 0.06
                        ELSE 0
                    END) AS laole_07
            FROM
              (SELECT *
               FROM fuse)a

        fuse == SELECT bb.rowkey,
                   bb.content_type,
                   bb.ht,
                   bb.plot1,
                   bb.plot2,
                   bb.plot3,
                   bb.plot4,
                   bb.plot5 from(
                     (SELECT c.rowkey,c.content_type,t.ht FROM content c
                      LEFT JOIN time t ON c.rowkey = t.rowkey
                      )aa
                      LEFT JOIN plot p ON aa.rowkey = p.device_id
                   )bb

SELECT device_id AS rowkey,
       count(DISTINCT dt) AS tj_active_level
FROM %s.dwd_portrait_playdetail_dtr
WHERE dt>='20220701'
  AND 20220731<='%s'
GROUP BY device_id

SELECT device_id AS rowkey,
       count(DISTINCT video_name) AS tj_switch_frequency
FROM knowyou_jituan_dmt.dwd_portrait_playdetail_dtr
WHERE dt>='20211213'
  AND dt<='20211226'
GROUP BY device_id

SELECT device_id AS rowkey,
       sum(watch_duration) AS tj_watch_frequency
FROM %s.dwd_portrait_playdetail_dtr
WHERE dt>='20220701'
  AND 20220731<='%s'
GROUP BY device_id


-- rec_group_category_hot
SELECT m.group_id,
       m.video_id,
       m.category,
       m.rating,
       m.license,
       m.date_time from
  (SELECT *,row_number() over(partition BY tt.group_id,tt.category ORDER BY tt.rating DESC)AS rank from
     (SELECT t.group_id,t.video_id,t.content_type AS category, sum(t.watch_count) AS rating ,t.license,t.date_time from
        (SELECT t0.video_id,t0.watch_count,t0.content_type,t1.group_id,t0.license,t0.dt AS date_time from
           (SELECT device_id ,video_id ,watch_count ,content_type,license,dt
            FROM %s.ads_rec_userdetail_wtr
            WHERE license ='%s'
              AND dt='%s')t0
         LEFT JOIN
           (SELECT group_id,device_id ,license,date_time
            FROM recGroupUserList)t1 ON t0.device_id=t1.device_id)t
      GROUP BY t.group_id , t.video_id , t.content_type , t.license , t.date_time)tt)m
WHERE m.rank<=50 


-- rec_group_label
SELECT t.group_id,
       round((sum(t.watch_count)/count(DISTINCT t.device_id)),4) AS avg_watchcount,
       round((sum(t.watch_duration)/count(DISTINCT t.device_id)/3600),4) AS avg_watchtime,
       t.license,
       t.date_time from
  (SELECT t0.device_id,t0.watch_count,t0.watch_duration,t1.group_id,t0.license,t0.dt AS date_time from
     (SELECT device_id ,watch_count ,watch_duration ,license,dt
      FROM %s.ads_rec_userdetail_wtr
      WHERE license ='%s'
        AND dt='%s')t0
   LEFT JOIN
     (SELECT group_id,device_id ,license,date_time
      FROM recGroupUserList)t1 ON t0.device_id=t1.device_id)t
GROUP BY t.group_id ,
         t.license ,
         t.date_time


# portrait_tag.sql
#!/bin/bash
#

with fuse as
(
  select 
    rowkey,  content_type,  ht,     plot1, 
    plot2,   plot3,         plot4,  plot5 
  from
  (
    (
      select c.rowkey,c.content_type,t.ht 
      from 
      (
        select b.device_id as rowkey, b.content_type 
        from 
        (
          select a.*,row_number() over(partition by device_id order by playtime desc)rank 
          from 
          (
            select device_id,content_type,sum(watch_duration) as playtime 
            from knowyou_jituan_dmt.dwd_portrait_playdetail_dtr 
            where dt>='20220801' and dt<='20220824' and length(device_id) > 0  and license = 3 and video_type = '2'
            group by device_id,content_type
          )a
        )b where b.rank=1
      ) c 
      left join 
      (
        select gg.device_id as rowkey,ht 
        from 
        (
          select ff.*,row_number() over (partition by device_id order by shichang desc)rank 
          from 
          (
            select a.device_id,a.ht,sum(watch_duration) as shichang 
            from 
            (
              select device_id,ht,watch_duration 
              from knowyou_jituan_dmt.dwd_portrait_playdetail_dtr 
              where dt>='20220801' and dt<='20220824' and length(device_id) > 0 and license = 3 and video_type = '2'
            )a group by a.device_id,a.ht
          )ff
        )gg where gg.rank=1
      ) t 
      on c.rowkey = t.rowkey
    )aa 
    left join 
    (
      select h.device_id,
        split(h.plot,',')[0] as plot1,
        split(h.plot,',')[1] as plot2,
        split(h.plot,',')[2] as plot3,
        split(h.plot,',')[3] as plot4,
        split(h.plot,',')[4] as plot5 
      from 
      (
        select device_id,concat_ws(',',collect_list(g.plot)) as plot 
        from 
        (
          select device_id,plot 
          from 
          (
            select f.*,row_number() over(partition by device_id order by cishu desc) rank 
            from 
            (
              select dd.device_id,dd.plot,count(*) as cishu 
              from 
              (
                select c.device_id,plot from 
                (
                  select b.device_id,b.video_name,a.video_plot 
                  from knowyou_jituan_dmt.dwd_portrait_playdetail_dtr b 
                  left join 
                  (
                    select video_name,video_plot 
                    from knowyou_jituan_dmt.dwd_portrait_videoinfo_dtr 
                    where dt>='20220801' and dt<='20220824' and license = 3
                  )a on b.video_name = a.video_name 
                  where dt>='20220801' and dt<='20220824' and length(device_id) > 0 and video_plot is not null and license = 3 and video_type = '2'
                )c lateral view explode(split(c.video_plot,'\\|')) demo as plot
              )dd 
              group by dd.device_id,dd.plot
            )f
          )h where h.rank <=5 
        )g group by device_id
      )h
    ) p 
    on aa.rowkey = p.device_id
  ) 
)
insert into table test.portrait_tag 
select rowkey, tag, '08', '2' from
(
  select 
    b.rowkey,
    (case when b.content_type='少儿' then '家有萌宝' 
      when b.content_type='教育' then '莘莘学子'
      when s.bailin>=s.woman and s.bailin>=s.zhongnian and s.bailin>=s.laole then '白领男士' 
      when s.woman>=s.bailin and s.woman>=s.zhongnian and s.woman>=s.laole then '时尚女性' 
      when s.zhongnian>=s.bailin and s.zhongnian>=s.woman and s.zhongnian>=s.laole then '人到中年' 
      when s.laole>=s.bailin and s.laole>=s.woman and s.laole>=s.zhongnian then '老有所乐' 
    end) as tag
  from 
  (
    select 
      rowkey,
      (case when bailin_03=0 and bailin_04=0 and bailin_05=0 and bailin_06=0 and bailin_07=0 then 0.7 
        else bailin_01+bailin_02+bailin_03+bailin_04+bailin_05+bailin_06+bailin_07+0.06 end) as bailin,
      (case when woman_03=0 and woman_04=0 and woman_05=0 and woman_06=0 and woman_07=0 then 0.7 
        else woman_01+woman_02+woman_03+woman_04+woman_05+woman_06+woman_07+0.06 end) as woman,
      (case when zhongnian_03=0 and zhongnian_04=0 and zhongnian_05=0 and zhongnian_06=0 and zhongnian_07=0 then 0.7 
        else zhongnian_01+zhongnian_02+zhongnian_03+zhongnian_04+zhongnian_05+zhongnian_06+zhongnian_07+0.06 end) as zhongnian,
      (case when laole_03=0 and laole_04=0 and laole_05=0 and laole_06=0 and laole_07=0 then 0.7 
        else laole_01+laole_02+laole_03+laole_04+laole_05+laole_06+laole_07+0.06 end) as laole 
    from 
    (
      select * from 
      (
        select a.rowkey,
          (case when content_type in ('电影','体育') then 0.5 else 0 end) as bailin_01,
          (case when ht in ('19','20','21','22','23','24','01') then 0.2 else 0 end) as bailin_02,
          (case when plot1 in ('科幻','动作','悬疑','犯罪武侠','战争','恐怖','足球','篮球','赛事','历史','人文','职场','都市','纪实','综艺','旅游') then 0.06 else 0 end) as bailin_03,
          (case when plot2 in ('科幻','动作','悬疑','犯罪武侠','战争','恐怖','足球','篮球','赛事','历史','人文','职场','都市','纪实','综艺','旅游') then 0.06 else 0 end) as bailin_04,
          (case when plot3 in ('科幻','动作','悬疑','犯罪武侠','战争','恐怖','足球','篮球','赛事','历史','人文','职场','都市','纪实','综艺','旅游') then 0.06 else 0 end) as bailin_05,
          (case when plot4 in ('科幻','动作','悬疑','犯罪武侠','战争','恐怖','足球','篮球','赛事','历史','人文','职场','都市','纪实','综艺','旅游') then 0.06 else 0 end) as bailin_06,
          (case when plot5 in ('科幻','动作','悬疑','犯罪武侠','战争','恐怖','足球','篮球','赛事','历史','人文','职场','都市','纪实','综艺','旅游') then 0.06 else 0 end) as bailin_07,
          (case when content_type in ('电影','电视剧','综艺') then 0.5 else 0 end) as woman_01,
          (case when ht in ('19','20','21','22','23','24','01') then 0.2 else 0 end) as woman_02,
          (case when plot1 in ('古装','都市','时尚','穿越','爱情','喜剧','青春','真人秀','偶像','女性','仙侠','综艺','校园','韩剧','文艺','旅游','音乐') then 0.06 else 0 end) as woman_03,
          (case when plot2 in ('古装','都市','时尚','穿越','爱情','喜剧','青春','真人秀','偶像','女性','仙侠','综艺','校园','韩剧','文艺','旅游','音乐') then 0.06 else 0 end) as woman_04,
          (case when plot3 in ('古装','都市','时尚','穿越','爱情','喜剧','青春','真人秀','偶像','女性','仙侠','综艺','校园','韩剧','文艺','旅游','音乐') then 0.06 else 0 end) as woman_05,
          (case when plot4 in ('古装','都市','时尚','穿越','爱情','喜剧','青春','真人秀','偶像','女性','仙侠','综艺','校园','韩剧','文艺','旅游','音乐') then 0.06 else 0 end) as woman_06,
          (case when plot5 in ('古装','都市','时尚','穿越','爱情','喜剧','青春','真人秀','偶像','女性','仙侠','综艺','校园','韩剧','文艺','旅游','音乐') then 0.06 else 0 end) as woman_07,
          (case when content_type in ('电影','电视剧','新闻','纪实') then 0.5 else 0 end) as zhongnian_01,
          (case when ht in ('13','14','15','16','17','18','19') then 0.5 else 0 end) as zhongnian_02,
          (case when plot1 in ('军旅','抗战','警匪','战争','猎奇','纪实','历史','文化','探索','新闻') then 0.06 else 0 end) as zhongnian_03,
          (case when plot2 in ('军旅','抗战','警匪','战争','猎奇','纪实','历史','文化','探索','新闻') then 0.06 else 0 end) as zhongnian_04,
          (case when plot3 in ('军旅','抗战','警匪','战争','猎奇','纪实','历史','文化','探索','新闻') then 0.06 else 0 end) as zhongnian_05,
          (case when plot4 in ('军旅','抗战','警匪','战争','猎奇','纪实','历史','文化','探索','新闻') then 0.06 else 0 end) as zhongnian_06,
          (case when plot5 in ('军旅','抗战','警匪','战争','猎奇','纪实','历史','文化','探索','新闻') then 0.06 else 0 end) as zhongnian_07,
          (case when content_type in ('电视剧','生活','健康') then 0.5 else 0 end) as laole_01,
          (case when ht in ('07','08','09','10','11','12','13') then 0.2 else 0 end) as laole_02,
          (case when plot1 in ('军旅','抗战','战争','家庭','伦理','健康','养生','新闻') then 0.06 else 0 end) as laole_03,
          (case when plot2 in ('军旅','抗战','战争','家庭','伦理','健康','养生','新闻') then 0.06 else 0 end) as laole_04,
          (case when plot3 in ('军旅','抗战','战争','家庭','伦理','健康','养生','新闻') then 0.06 else 0 end) as laole_05,
          (case when plot4 in ('军旅','抗战','战争','家庭','伦理','健康','养生','新闻') then 0.06 else 0 end) as laole_06,
          (case when plot5 in ('军旅','抗战','战争','家庭','伦理','健康','养生','新闻') then 0.06 else 0 end) as laole_07 
        from 
        (
          select * from fuse
        )a
      ) b
    ) c
  ) s 
  left join fuse b 
  on s.rowkey=b.rowkey
) z
group by rowkey, tag
;


select 
  device_id,     
  video_name,    
  video_type,    
  content_type,  
  create_time,   
  watch_duration,
  video_length,  
  license,       
  ht            
from knowyou_jituan_dmt.dwd_portrait_playdetail_dtr 
where dt=20220825

select 
  video_id    ,
  video_name  ,
  content_type,
  video_plot  ,
  video_region,
  direct_name ,
  actor_name  ,
  release_date,
  license 
from knowyou_jituan_dmt.dwd_portrait_videoinfo_dtr 
where dt=20220825 




INSERT OVERWRITE TABLE knowyou_jituan_dmt.dwd_portrait_playdetail_dtr partition(dt)
select a.device_id,a.video_name,
(
case a.video_type     
     when '直播' then '1'
     when '点播' then '2'
     when '回看' then '3'
     else '0'
     end 
) as video_type,
(case b.contenttype 
        when '电视剧' then '电视剧'
        when '电影' then '电影'
        when '综艺' then '综艺' 
        when '少儿' then '少儿'
        when '动漫' then '动漫' 
        when '纪实' then '纪实' 
        when '教育' then '教育' 
        when '体育' then '体育' 
        when '生活' then '生活'
        else '其他'
    end) as content_type,a.create_time,
    if((a.watch_duration is null or a.watch_duration=''),'0',a.watch_duration) as watch_duration,
    if((b.duration is null or b.duration=''),0,(b.duration*60)) as  video_length,  
    a.cp_name as license,a.ht,a.dt  from (
select deviceid as device_id,videoname as video_name,videotype as video_type,
play_starttime as create_time,
playtime as watch_duration,
from_unixtime(unix_timestamp(play_endtime,'yyyyMMddHHmmss'),'HH')  as ht,
date_time as dt,
contentidfromepg,
cp_name
from knowyou_jituan_edw.edw_uba_cn_videoplayinfo
where date_time ='20220801' and  playstatus = '1' ) a  -- 1==播放结束（播放成功）
left join knowyou_jituan_edw.edw_ba_cn_videoinformation  b
on a.video_name = b.videoname and a.cp_name  =  b.cp_name

====================================================================================================

INSERT OVERWRITE TABLE knowyou_jituan_dmt.dwd_portrait_videoinfo_dtr partition(dt)
select 
b.key as video_id,
a.videoname as video_name,
(case b.contenttype 
        when '电视剧' then '电视剧'
        when '电影' then '电影'
        when '综艺' then '综艺' 
        when '少儿' then '少儿'
        when '动漫' then '动漫' 
        when '纪实' then '纪实' 
        when '教育' then '教育' 
        when '体育' then '体育' 
        when '生活' then '生活'
        else '其他'
    end) as content_type,
regexp_replace(b.programClass,'\\/','\\|') as video_plot,
(case when b.region regexp('.*\\/.*')  then regexp_replace(b.region,'\\/','\\|')
      when b.region regexp('.*\\,.*') then regexp_replace(b.region,'\\,','\\|')  
      when b.region regexp('.* .*')  then  regexp_replace(b.region ,' ','\\|') 
      else b.region end  ) as video_region,
regexp_replace(b.directorname,"\\/","\\|") as direct_name,
regexp_replace(b.actorname,"\\/","\\|")  as actor_name, 
b.releasedate as release_date,
a.cp_name as license,
date_time as dt
from( select key ,videoname ,contenttype ,programClass ,trim(region) as region ,director directorname ,actor actorname ,datepublished releasedate ,cp_name
from  
   knowyou_jituan_edw.edw_ba_cn_videoinformation )b 
LEFT JOIN  ( select videoname,cp_name,date_time from knowyou_jituan_edw.edw_uba_cn_videoplayinfo 
where date_time >= '20220801' GROUP BY videoname,cp_name,date_time  ) a
on a.videoname = b.videoname and a.cp_name = b.cp_name
where a.videoname is not null 


====================================================================================================
CURRE_DAY+=(20220801)
CURRE_DAY+=(20220802)
CURRE_DAY+=(20220803)
CURRE_DAY+=(20220804)
CURRE_DAY+=(20220805)
CURRE_DAY+=(20220806)
CURRE_DAY+=(20220807)
CURRE_DAY+=(20220808)
CURRE_DAY+=(20220809)
CURRE_DAY+=(20220810)
CURRE_DAY+=(20220811)
CURRE_DAY+=(20220812)
CURRE_DAY+=(20220813)
CURRE_DAY+=(20220814)
CURRE_DAY+=(20220815)
CURRE_DAY+=(20220816)
CURRE_DAY+=(20220817)
CURRE_DAY+=(20220818)
CURRE_DAY+=(20220819)
CURRE_DAY+=(20220820)
CURRE_DAY+=(20220821)
CURRE_DAY+=(20220822)
CURRE_DAY+=(20220823)
CURRE_DAY+=(20220824)
CURRE_DAY+=(20220825)
CURRE_DAY+=(20220826)
CURRE_DAY+=(20220827)
CURRE_DAY+=(20220828)
CURRE_DAY+=(20220829)
CURRE_DAY+=(20220830)
CURRE_DAY+=()

for DAY in ${CURRE_DAY[@]}
do
  SEVEN_DAY=$(date -d"14 day ago ${DAY}" +%Y%m%d)
  echo "[$SEVEN_DAY, $DAY]"

  hive -hivevar SEVEN_DAY=${SEVEN_DAY} -hivevar DAY=${DAY} -f insert.sql
done

set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_jituan_dmt.ads_rec_userdetail_wtr partition(dt)
select deviceid as device_id,contentidfromepg as video_id,videoname as video_name,
(case contenttype
        when '电视剧' then '电视剧'
        when '电影' then '电影'
        when '综艺' then '综艺'
        when '少儿' then '少儿'
        when '动漫' then '动漫'
        when '纪实' then '纪实'
        when '教育' then '教育'
        when '体育' then '体育'
        when '生活' then '生活'
        else '其他'
    end) as content_type,
max(play_starttime) as last_watch_time,
sum(playtime) as watch_duration,
count(1) as watch_count,
cp_name as license,
${hivevar:DAY} as dt
from knowyou_jituan_edw.edw_uba_cn_videoplayinfo
where date_time >= ${hivevar:SEVEN_DAY} and date_time <= ${hivevar:DAY}
and playstatus='1' and videotype='点播'
group by deviceid,videoname,cp_name,contenttype,contentidfromepg;


INSERT OVERWRITE TABLE knowyou_jituan_dmt.ads_rec_userdetail_wtr partition(dt=20220831)
select 
device_id      
,video_id       
,video_name     
,content_type   
,last_watch_time
,watch_duration 
,watch_count    
,license        
from knowyou_jituan_dmt.ads_rec_userdetail_wtr where dt>=20220801


select count(distinct deviceid) from (
select deviceid, contentidfromepg from knowyou_jituan_edw.edw_uba_cn_videoplayinfo 
where cp_name ='3' and date_time>='20200801' and date_time<='20200830' and playstatus='1' and videotype='点播' and length(contentidfromepg) = 0
group by deviceid, contentidfromepg
) a;
select count(distinct deviceid) from (
select deviceid, contentidfromepg from knowyou_jituan_edw.edw_uba_cn_videoplayinfo 
where cp_name ='3' and date_time>='20220801' and playstatus='1' and date_time<='20200830' and videotype='点播' and length(contentidfromepg) > 0
group by deviceid, contentidfromepg
) a;
select count(distinct deviceid) from (
select deviceid, contentidfromepg from knowyou_jituan_edw.edw_uba_cn_videoplayinfo 
where cp_name ='3' and date_time>='20220801' and playstatus='1' and videotype='点播' 
and length(contentidfromepg) = 0 and length(videoname) = 0
group by deviceid, contentidfromepg
) a;

select deviceid, contentidfromepg,videoname from knowyou_jituan_edw.edw_uba_cn_videoplayinfo 
where cp_name ='3' and date_time>='20220801' and playstatus='1' and videotype='点播' and length(contentidfromepg) = 0

select count(distinct device_id) from (
select device_id, video_id  from knowyou_jituan_dmt.ads_rec_userdetail_wtr where license ='3' 
and dt>='20211101' and dt<='20211130'  
and length(video_id) = 0 group by device_id, video_id
) a;
