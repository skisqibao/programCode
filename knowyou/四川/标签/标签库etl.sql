--1.用户活跃信息基础标签表

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.HTV_SERV_BASIC PARTITION(date_time)
SELECT 
    a.deviceid AS deviceid,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS is_boot, -- 当日是否开机
    (CASE WHEN c.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS is_play, -- 当日是否播放
    (CASE WHEN c.play_duration IS NOT NULL THEN c.play_duration ELSE 0 END) AS play_duration, -- 当日播放时长
    (CASE WHEN c.play_num IS NOT NULL THEN c.play_num ELSE 0 END) AS play_num , -- 当日播放次数
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS is_live, -- 当日收看直播
    (CASE WHEN d.play_num IS NOT NULL THEN d.play_num ELSE 0 END) AS live_play_num, -- 当日收看直播次数
    (CASE WHEN d.play_duration IS NOT NULL THEN d.play_duration ELSE 0 END) AS live_play_duration, -- 当日收看直播时长
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS is_demand, -- 当日收看点播
    (CASE WHEN e.play_num IS NOT NULL THEN e.play_num ELSE 0 END) AS demand_play_num, --当日收看点播次数
    (CASE WHEN e.play_duration IS NOT NULL THEN e.play_duration ELSE 0 END) AS demand_play_duration, -- 当日收看点播时长
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS is_replay, -- 当日收看回看
    (CASE WHEN f.play_num IS NOT NULL THEN f.play_num ELSE 0 END) AS replay_play_num, --当日收看回看次数
    (CASE WHEN f.play_duration IS NOT NULL THEN f.play_duration ELSE 0 END) AS replay_play_duration, -- 当日收看回看时长 
    '20220615' AS date_time
FROM
(
    SELECT deviceid FROM knowyou_ott_edw.edw_mbh_deviceinfo WHERE date_time='20220615'
    GROUP BY deviceid
) a
-- 是否开机 
LEFT JOIN 
(
    SELECT deviceid FROM knowyou_ott_edw.edw_mbh_devicestate 
    WHERE date_time='20220615' AND active_state='1'
    GROUP BY deviceid
) b ON a.deviceid =b.deviceid
--当日是否播放\当日播放时长\当日播放次数
LEFT JOIN
( 
    SELECT deviceid, count(*) AS play_num, round(sum(playTime)/3600, 1) AS play_duration
    FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始'
    GROUP BY deviceid
) c ON a.deviceid=c.deviceid
-- 当日收看直播\次数\时长
LEFT JOIN
(
    SELECT deviceid, count(*) AS play_num, round(sum(playTime)/3600, 1) AS play_duration
    FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='直播'
    GROUP BY deviceid
) d ON a.deviceid=d.deviceid 
-- 当日收看点播\次数\时长
LEFT JOIN
(
    SELECT deviceid, count(*) AS play_num, round(sum(playTime)/3600, 1) AS play_duration
    FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
) e ON a.deviceid=e.deviceid
-- 当日收看回播\次数\时长
LEFT JOIN
(
    SELECT deviceid, count(*) AS play_num,round(sum(playTime)/3600, 1) AS play_duration
    FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='回看'
    GROUP BY deviceid
) f ON a.deviceid=f.deviceid



--当月活跃信息基础标签表

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.HTV_SERV_BASIC_MONTH PARTITION(date_time)
SELECT a.deviceid AS deviceid,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS is_boot, -- 当月是否开机
    (CASE WHEN c.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS is_play, -- 当月是否播放
    (CASE WHEN c.play_duration IS NOT NULL THEN c.play_duration ELSE 0 END) AS play_duration, -- 当月播放时长
    (CASE WHEN c.play_num IS NOT NULL THEN c.play_num ELSE 0 END) AS play_num , -- 当月播放次数
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS is_live, -- 当月收看直播
    (CASE WHEN d.play_num IS NOT NULL THEN d.play_num ELSE 0 END) AS live_play_num, -- 当月收看直播次数
    (CASE WHEN d.play_duration IS NOT NULL THEN d.play_duration ELSE 0 END) AS live_play_duration, -- 当月收看直播时长
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS is_demand, -- 当月收看点播
    (CASE WHEN e.play_num IS NOT NULL THEN e.play_num ELSE 0 END) AS demand_play_num, --当月收看点播次数
    (CASE WHEN e.play_duration IS NOT NULL THEN e.play_duration ELSE 0 END) AS demand_play_duration, -- 当月收看点播时长
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS is_replay, -- 当月收看回看
    (CASE WHEN f.play_num IS NOT NULL THEN f.play_num ELSE 0 END) AS replay_play_num, --当月收看回看次数
    (CASE WHEN f.play_duration IS NOT NULL THEN f.play_duration ELSE 0 END) AS replay_play_duration, -- 当月收看回看时长 
    '20220615' AS date_time
FROM
(
    SELECT deviceid FROM knowyou_ott_edw.edw_mbh_deviceinfo
    WHERE date_time<='20220615' AND date_time>='20220516'
) a
-- 是否开机
LEFT JOIN 
(
    SELECT deviceid FROM knowyou_ott_edw.edw_mbh_devicestate 
    WHERE date_time<='20220615' AND date_time>='20220516' AND active_state='1'
    GROUP BY deviceid
) b ON a.deviceid =b.deviceid
--当月是否播放\当月播放时长\当月播放次数
LEFT JOIN
( 
    SELECT deviceid,count(*) AS play_num,round(sum(playTime)/3600,1) AS play_duration
    FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始'
    GROUP BY deviceid
) c ON a.deviceid=c.deviceid
-- 当月收看直播\次数\时长
LEFT JOIN
(
    SELECT deviceid,count(*) AS play_num,round(sum(playTime)/3600,1) AS play_duration
    FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='直播'
    GROUP BY deviceid
) d ON a.deviceid=d.deviceid 
-- 当月收看点播\次数\时长
LEFT JOIN
(
    SELECT deviceid,count(*) AS play_num,round(sum(playTime)/3600,1) AS play_duration
    FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
) e ON a.deviceid=e.deviceid
-- 当月收看回播\次数\时长
LEFT JOIN
(
    SELECT deviceid,count(*) AS play_num,round(sum(playTime)/3600,1) AS play_duration
    FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='回看'
    GROUP BY deviceid
) f ON a.deviceid=f.deviceid



-- 2.当日直播时段偏好标签表
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_live_prefer PARTITION(date_time)
SELECT 
    a.deviceid AS deviceid,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS live_prefer_lv1,
    (CASE WHEN c.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS live_prefer_lv2,
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS live_prefer_lv3,
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS live_prefer_lv4,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS live_prefer_lv5,
    (CASE WHEN g.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS live_prefer_lv6,
    (CASE WHEN h.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS live_prefer_lv7,
    '20220615' AS date_time
FROM
(
    SELECT deviceid FROM knowyou_ott_edw.edw_mbh_deviceinfo 
    WHERE date_time='20220615'
) a
-- 当日直播时段0-6点偏好
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='直播'
    AND substr(actiontime,9,2)>='00' AND substr(actiontime,9,2)<'06'
    GROUP BY deviceid
) b ON a.deviceid=b.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='直播'
    AND substr(actiontime,9,2)>='06' AND substr(actiontime,9,2)<'09'
    GROUP BY deviceid
) c ON a.deviceid=c.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='直播'
    AND substr(actiontime,9,2)>='09' AND substr(actiontime,9,2)<'12'
    GROUP BY deviceid
) d ON a.deviceid=d.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='直播'
    AND substr(actiontime,9,2)>='12' AND substr(actiontime,9,2)<'14'
    GROUP BY deviceid
) e ON a.deviceid=e.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='直播'
    AND substr(actiontime,9,2)>='14' AND substr(actiontime,9,2)<'18'
    GROUP BY deviceid
) f ON a.deviceid=f.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='直播'
    AND substr(actiontime,9,2)>='18' AND substr(actiontime,9,2)<'22'
    GROUP BY deviceid
) g ON a.deviceid=g.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='直播'
    AND substr(actiontime,9,2)>='22' AND substr(actiontime,9,2)<'24'
    GROUP BY deviceid
) h ON a.deviceid=h.deviceid



--数据不够，查月报错 当月直播时段偏好标签表
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_live_prefer_month PARTITION(date_time)
SELECT 
    a.deviceid AS deviceid,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS live_prefer_lv1,
    (CASE WHEN c.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS live_prefer_lv2,
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS live_prefer_lv3,
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS live_prefer_lv4,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS live_prefer_lv5,
    (CASE WHEN g.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS live_prefer_lv6,
    (CASE WHEN h.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS live_prefer_lv7,
    '20220615' AS date_time
FROM
(
    SELECT deviceid FROM knowyou_ott_edw.edw_mbh_deviceinfo
    WHERE date_time<='20220615' AND date_time>='20220516'
    GROUP BY deviceid
) a
-- 当月直播时段0-6点偏好
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='直播'
    AND substr(actiontime,9,2)>='00' AND substr(actiontime,9,2)<'06'
    GROUP BY deviceid
)b ON a.deviceid=b.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='直播'
    AND substr(actiontime,9,2)>='06' AND substr(actiontime,9,2)<'09'
    GROUP BY deviceid
)c ON a.deviceid=c.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='直播'
    AND substr(actiontime,9,2)>='09' AND substr(actiontime,9,2)<'12'
    GROUP BY deviceid
)d ON a.deviceid=d.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='直播'
    AND substr(actiontime,9,2)>='12' AND substr(actiontime,9,2)<'14'
    GROUP BY deviceid
)e ON a.deviceid=e.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='直播'
    AND substr(actiontime,9,2)>='14' AND substr(actiontime,9,2)<'18'
    GROUP BY deviceid
)f ON a.deviceid=f.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='直播'
    AND substr(actiontime,9,2)>='18' AND substr(actiontime,9,2)<'22'
    GROUP BY deviceid
)g ON a.deviceid=g.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='直播'
    AND substr(actiontime,9,2)>='22' AND substr(actiontime,9,2)<'24'
    GROUP BY deviceid
)h ON a.deviceid=h.deviceid


-- 3.点播时段偏好标签表
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_demand_prefer PARTITION(date_time)
SELECT 
    a.deviceid,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_prefer_lv1,
    (CASE WHEN c.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_prefer_lv2,
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_prefer_lv3,
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_prefer_lv4,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_prefer_lv5,
    (CASE WHEN g.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_prefer_lv6,
    (CASE WHEN h.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_prefer_lv7,
    '20220615' AS date_time
FROM
(
    SELECT deviceid FROM knowyou_ott_edw.edw_mbh_deviceinfo
    WHERE date_time='20220615'
    GROUP BY deviceid
)a
LEFT JOIN
(
    -- 当日点播时段0-6点偏好
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='点播'
    AND substr(actiontime,9,2)>='00' AND substr(actiontime,9,2)<'06'
    GROUP BY deviceid
)b ON a.deviceid=b.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='点播'
    AND substr(actiontime,9,2)>='06' AND substr(actiontime,9,2)<'09'
    GROUP BY deviceid
)c ON a.deviceid=c.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='点播'
    AND substr(actiontime,9,2)>='09' AND substr(actiontime,9,2)<'12'
    GROUP BY deviceid
)d ON a.deviceid=d.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='点播'
    AND substr(actiontime,9,2)>='12' AND substr(actiontime,9,2)<'14'
    GROUP BY deviceid
)e ON a.deviceid=e.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='点播'
    AND substr(actiontime,9,2)>='14' AND substr(actiontime,9,2)<'18'
    GROUP BY deviceid
)f ON a.deviceid=f.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='点播'
    AND substr(actiontime,9,2)>='18' AND substr(actiontime,9,2)<'22'
    GROUP BY deviceid
)g ON a.deviceid=g.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='点播'
    AND substr(actiontime,9,2)>='22' AND substr(actiontime,9,2)<'24'
    GROUP BY deviceid
)h ON a.deviceid=h.deviceid



--报错当月数据查不到 当月点播时段偏好标签表
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_demand_prefer_month PARTITION(date_time)
SELECT 
    a.deviceid AS deviceid,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_prefer_lv1,
    (CASE WHEN c.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_prefer_lv2,
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_prefer_lv3,
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_prefer_lv4,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_prefer_lv5,
    (CASE WHEN g.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_prefer_lv6,
    (CASE WHEN h.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_prefer_lv7,
    '20220615' AS date_time
FROM(
    SELECT deviceid FROM knowyou_ott_edw.edw_mbh_deviceinfo
    WHERE date_time<='20220615' AND date_time>='20220516'
    GROUP BY deviceid
)a
LEFT JOIN
(
    -- 当月点播时段0-6点偏好
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='点播'
    AND substr(actiontime,9,2)>='00' AND substr(actiontime,9,2)<'06'
    GROUP BY deviceid
)b ON a.deviceid=b.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='点播'
    AND substr(actiontime,9,2)>='06' AND substr(actiontime,9,2)<'09'
    GROUP BY deviceid
)c ON a.deviceid=c.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='点播'
    AND substr(actiontime,9,2)>='09' AND substr(actiontime,9,2)<'12'
    GROUP BY deviceid
)d ON a.deviceid=d.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='点播'
    AND substr(actiontime,9,2)>='12' AND substr(actiontime,9,2)<'14'
    GROUP BY deviceid
)e ON a.deviceid=e.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='点播'
    AND substr(actiontime,9,2)>='14' AND substr(actiontime,9,2)<'18'
    GROUP BY deviceid
)f ON a.deviceid=f.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='点播'
    AND substr(actiontime,9,2)>='18' AND substr(actiontime,9,2)<'22'
    GROUP BY deviceid
)g ON a.deviceid=g.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='点播'
    AND substr(actiontime,9,2)>='22' AND substr(actiontime,9,2)<'24'
    GROUP BY deviceid
)h ON a.deviceid=h.deviceid


-- 4.回看时段偏好标签表
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_replay_prefer PARTITION(date_time)
SELECT  
    a.deviceid AS deviceid,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS replay_prefer_lv1,
    (CASE WHEN c.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS replay_prefer_lv2,
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS replay_prefer_lv3,
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS replay_prefer_lv4,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS replay_prefer_lv5,
    (CASE WHEN g.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS replay_prefer_lv6,
    (CASE WHEN h.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS replay_prefer_lv7,
    '20220615' AS date_time
FROM(
    SELECT deviceid FROM knowyou_ott_edw.edw_mbh_deviceinfo
    WHERE date_time='20220615'
)a
    -- 当日回看时段0-6点偏好
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='回看'
    AND substr(actiontime,9,2)>='00' AND substr(actiontime,9,2)<'06'
    GROUP BY deviceid
)b ON a.deviceid=b.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='回看'
    AND substr(actiontime,9,2)>='06' AND substr(actiontime,9,2)<'09'
    GROUP BY deviceid
)c ON a.deviceid=c.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='回看'
    AND substr(actiontime,9,2)>='09' AND substr(actiontime,9,2)<'12'
    GROUP BY deviceid
)d ON a.deviceid=d.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='回看'
    AND substr(actiontime,9,2)>='12' AND substr(actiontime,9,2)<'14'
    GROUP BY deviceid
)e ON a.deviceid=e.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='回看'
    AND substr(actiontime,9,2)>='14' AND substr(actiontime,9,2)<'18'
    GROUP BY deviceid
)f ON a.deviceid=f.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='回看'
    AND substr(actiontime,9,2)>='18' AND substr(actiontime,9,2)<'22'
    GROUP BY deviceid
)g ON a.deviceid=g.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer 
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='回看'
    AND substr(actiontime,9,2)>='22' AND substr(actiontime,9,2)<'24'
    GROUP BY deviceid
)h ON a.deviceid=h.deviceid



-- 当月回看时段偏好标签表
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_replay_prefer_month PARTITION(date_time)
SELECT  
    a.deviceid AS deviceid,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS replay_prefer_lv1,
    (CASE WHEN c.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS replay_prefer_lv2,
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS replay_prefer_lv3,
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS replay_prefer_lv4,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS replay_prefer_lv5,
    (CASE WHEN g.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS replay_prefer_lv6,
    (CASE WHEN h.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS replay_prefer_lv7,
    '20220615' AS date_time
FROM
(
    SELECT deviceid FROM knowyou_ott_edw.edw_mbh_deviceinfo
    WHERE date_time<='20220615' AND date_time>='20220516'
    GROUP BY deviceid
)a
    -- 当月回看时段0-6点偏好
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='回看'
    AND substr(actiontime,9,2)>='00' AND substr(actiontime,9,2)<'06'
    GROUP BY deviceid
)b ON a.deviceid=b.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='回看'
    AND substr(actiontime,9,2)>='06' AND substr(actiontime,9,2)<'09'
    GROUP BY deviceid
)c ON a.deviceid=c.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='回看'
    AND substr(actiontime,9,2)>='09' AND substr(actiontime,9,2)<'12'
    GROUP BY deviceid
)d ON a.deviceid=d.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='回看'
    AND substr(actiontime,9,2)>='12' AND substr(actiontime,9,2)<'14'
    GROUP BY deviceid
)e ON a.deviceid=e.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='回看'
    AND substr(actiontime,9,2)>='14' AND substr(actiontime,9,2)<'18'
    GROUP BY deviceid
)f ON a.deviceid=f.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='回看'
    AND substr(actiontime,9,2)>='18' AND substr(actiontime,9,2)<'22'
    GROUP BY deviceid
)g ON a.deviceid=g.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer 
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='回看'
    AND substr(actiontime,9,2)>='22' AND substr(actiontime,9,2)<'24'
    GROUP BY deviceid
)h ON a.deviceid=h.deviceid



-- 当日直播频道偏好标签中间表
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_channel_dm PARTITION(date_time)
SELECT  
    a.deviceid,
    a.channelname,
    sum(a.playtime) AS play_duration,
    a.date_time AS date_time
FROM
(
    SELECT deviceid,
     (CASE WHEN channelname IN ('CCTV-1','cctv-1','ysten-cctv-1') THEN 'CCTV1'
     WHEN channelname IN ('CCTV-2','cctv-2') THEN 'CCTV2'
     WHEN channelname IN ('CCTV-3','cctv-3') THEN 'CCTV3'
     WHEN channelname IN ('CCTV-4','cctv-4') THEN 'CCTV4'
     WHEN channelname IN ('CCTV-5','cctv-5','cctv5+','ysten-cctv-5','ysten-cctv5plus','CCTV5+') THEN 'CCTV5'
     WHEN channelname IN ('CCTV-6','cctv-6') THEN 'CCTV6'
     WHEN channelname IN ('CCTV-7','cctv-7') THEN 'CCTV7'
     WHEN channelname IN ('CCTV-8','cctv-8') THEN 'CCTV8'
     WHEN channelname IN ('CCTV-9','cctv-9') THEN 'CCTV9'
     WHEN channelname IN ('CCTV-10','cctv-10') THEN 'CCTV10'
     WHEN channelname IN ('CCTV-11','cctv-11') THEN 'CCTV11'
     WHEN channelname IN ('CCTV-12','cctv-12') THEN 'CCTV12'
     WHEN channelname IN ('CCTV-13','cctv-13') THEN 'CCTV13'
     WHEN channelname IN ('CCTV-14','cctv-14') THEN 'CCTV14'
     WHEN channelname IN ('CCTV-15','cctv-15') THEN 'CCTV15'
     WHEN channelname IN ('CCTV-16','cctv-16') THEN 'CCTV16'
     WHEN channelname IN ('CCTV-17','cctv-17') THEN 'CCTV17'
     WHEN channelname IN ('江苏卫视','jiangsustv')THEN '江苏卫视'
     WHEN channelname IN ('浙江卫视','zhejiangstv')THEN '浙江卫视'
     WHEN channelname IN ('湖南卫视','hunanstv')THEN '湖南卫视'
     WHEN channelname IN ('北京卫视','beijingstv')THEN '北京卫视'
     WHEN channelname IN ('深圳卫视','shenzhenstv')THEN '深圳卫视'
     WHEN channelname IN ('辽宁卫视','liaoningstv')THEN '辽宁卫视'
     WHEN channelname IN ('湖北卫视','hubeistv')THEN '湖北卫视'
     WHEN channelname IN ('安徽卫视','anhuistv')THEN '安徽卫视'
     WHEN channelname IN ('重庆卫视','chongqingstv')THEN '重庆卫视'
     WHEN channelname IN ('天津卫视','tianjinstv')THEN '天津卫视'
     WHEN channelname IN ('黑龙江卫视','heilongjiangstv')THEN '黑龙江卫视'
     WHEN channelname IN ('吉林卫视','jilinstv')THEN '吉林卫视'
     WHEN channelname IN ('内蒙古卫视','neimenggustv')THEN '内蒙古卫视'
     WHEN channelname IN ('河北卫视','hebeistv')THEN '河北卫视'
     WHEN channelname IN ('河南卫视','henanstv')THEN '河南卫视'
     WHEN channelname IN ('江西卫视','jiangxistv')THEN '江西卫视'
     WHEN channelname IN ('广东卫视','guangdongstv')THEN '广东卫视'
     WHEN channelname IN ('广西卫视','guangxistv')THEN '广西卫视'
     WHEN channelname IN ('东南卫视','dongnanstv')THEN '东南卫视'
     WHEN channelname IN ('云南卫视','yunnanstv')THEN '云南卫视'
     WHEN channelname IN ('贵州卫视','guizhoustv')THEN '贵州卫视'
     WHEN channelname IN ('山西卫视','shanxistv')THEN '山西卫视'
     WHEN channelname IN ('山东卫视','shandongstv')THEN '山东卫视'
     WHEN channelname IN ('宁夏卫视','ningxiastv')THEN '宁夏卫视'
     ELSE '其他直播频道' END ) AS channelname,
     playtime,
     date_time
    FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND playstatus='开始' AND videotype='直播'
)a
GROUP BY a.deviceid, a.channelname, a.date_time;



-- 当月直播频道偏好标签中间表
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_channel_dm_month PARTITION(date_time)
SELECT  
    a.deviceid,
    a.channelname,
    sum(a.playtime) AS play_duration,
    a.date_time AS date_time
FROM(
    SELECT 
        deviceid,
        (CASE WHEN channelname IN ('CCTV-1','cctv-1','ysten-cctv-1') THEN 'CCTV1'
        WHEN channelname IN ('CCTV-2','cctv-2') THEN 'CCTV2'
        WHEN channelname IN ('CCTV-3','cctv-3') THEN 'CCTV3'
        WHEN channelname IN ('CCTV-4','cctv-4') THEN 'CCTV4'
        WHEN channelname IN ('CCTV-5','cctv-5','cctv5+','ysten-cctv-5','ysten-cctv5plus','CCTV5+') THEN 'CCTV5'
        WHEN channelname IN ('CCTV-6','cctv-6') THEN 'CCTV6'
        WHEN channelname IN ('CCTV-7','cctv-7') THEN 'CCTV7'
        WHEN channelname IN ('CCTV-8','cctv-8') THEN 'CCTV8'
        WHEN channelname IN ('CCTV-9','cctv-9') THEN 'CCTV9'
        WHEN channelname IN ('CCTV-10','cctv-10') THEN 'CCTV10'
        WHEN channelname IN ('CCTV-11','cctv-11') THEN 'CCTV11'
        WHEN channelname IN ('CCTV-12','cctv-12') THEN 'CCTV12'
        WHEN channelname IN ('CCTV-13','cctv-13') THEN 'CCTV13'
        WHEN channelname IN ('CCTV-14','cctv-14') THEN 'CCTV14'
        WHEN channelname IN ('CCTV-15','cctv-15') THEN 'CCTV15'
        WHEN channelname IN ('CCTV-16','cctv-16') THEN 'CCTV16'
        WHEN channelname IN ('CCTV-17','cctv-17') THEN 'CCTV17'
        WHEN channelname IN ('江苏卫视','jiangsustv')THEN '江苏卫视'
        WHEN channelname IN ('浙江卫视','zhejiangstv')THEN '浙江卫视'
        WHEN channelname IN ('湖南卫视','hunanstv')THEN '湖南卫视'
        WHEN channelname IN ('北京卫视','beijingstv')THEN '北京卫视'
        WHEN channelname IN ('深圳卫视','shenzhenstv')THEN '深圳卫视'
        WHEN channelname IN ('辽宁卫视','liaoningstv')THEN '辽宁卫视'
        WHEN channelname IN ('湖北卫视','hubeistv')THEN '湖北卫视'
        WHEN channelname IN ('安徽卫视','anhuistv')THEN '安徽卫视'
        WHEN channelname IN ('重庆卫视','chongqingstv')THEN '重庆卫视'
        WHEN channelname IN ('天津卫视','tianjinstv')THEN '天津卫视'
        WHEN channelname IN ('黑龙江卫视','heilongjiangstv')THEN '黑龙江卫视'
        WHEN channelname IN ('吉林卫视','jilinstv')THEN '吉林卫视'
        WHEN channelname IN ('内蒙古卫视','neimenggustv')THEN '内蒙古卫视'
        WHEN channelname IN ('河北卫视','hebeistv')THEN '河北卫视'
        WHEN channelname IN ('河南卫视','henanstv')THEN '河南卫视'
        WHEN channelname IN ('江西卫视','jiangxistv')THEN '江西卫视'
        WHEN channelname IN ('广东卫视','guangdongstv')THEN '广东卫视'
        WHEN channelname IN ('广西卫视','guangxistv')THEN '广西卫视'
        WHEN channelname IN ('东南卫视','dongnanstv')THEN '东南卫视'
        WHEN channelname IN ('云南卫视','yunnanstv')THEN '云南卫视'
        WHEN channelname IN ('贵州卫视','guizhoustv')THEN '贵州卫视'
        WHEN channelname IN ('山西卫视','shanxistv')THEN '山西卫视'
        WHEN channelname IN ('山东卫视','shandongstv')THEN '山东卫视'
        WHEN channelname IN ('宁夏卫视','ningxiastv')THEN '宁夏卫视'
        ELSE '其他直播频道' END ) AS channelname,
        playtime,
        '20220615' date_time
    FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND playstatus='开始' AND videotype='直播'
)a
GROUP BY a.deviceid, a.channelname, a.date_time;



--5 当日直播CCTV频道偏好标签表
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_channel_prefer_cctv_dm PARTITION(date_time='20220615')
SELECT  
    a.key AS deviceid ,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_cctv1,
    (CASE WHEN b.deviceid IS NOT NULL AND b.play_duration<3600 THEN 1 ELSE 0 END) AS channel_low_prefer_cctv1,
    (CASE WHEN b.deviceid IS NOT NULL AND b.play_duration>=3600 AND b.play_duration<=10800 THEN 1 ELSE 0 END) AS channel_middle_prefer_cctv1,
    (CASE WHEN b.deviceid IS NOT NULL AND b.play_duration>10800 THEN 1 ELSE 0 END) AS channel_high_prefer_cctv1,
    (CASE WHEN c.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_cctv2,
    (CASE WHEN c.deviceid IS NOT NULL AND c.play_duration<3600 THEN 1 ELSE 0 END) AS channel_low_prefer_cctv2,
    (CASE WHEN c.deviceid IS NOT NULL AND c.play_duration>=3600 AND c.play_duration<=10800 THEN 1 ELSE 0 END) AS channel_middle_prefer_cctv2,
    (CASE WHEN c.deviceid IS NOT NULL AND c.play_duration>10800 THEN 1 ELSE 0 END) AS channel_high_prefer_cctv2,
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_cctv3,
    (CASE WHEN d.deviceid IS NOT NULL AND d.play_duration<3600 THEN 1 ELSE 0 END) AS channel_low_prefer_cctv3,
    (CASE WHEN d.deviceid IS NOT NULL AND d.play_duration>=3600 AND d.play_duration<=10800 THEN 1 ELSE 0 END) AS channel_middle_prefer_cctv3,
    (CASE WHEN d.deviceid IS NOT NULL AND d.play_duration>10800 THEN 1 ELSE 0 END) AS channel_high_prefer_cctv3,
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_cctv4,
    (CASE WHEN e.deviceid IS NOT NULL AND d.play_duration<3600 THEN 1 ELSE 0 END) AS channel_low_prefer_cctv4,
    (CASE WHEN e.deviceid IS NOT NULL AND d.play_duration>=3600 AND d.play_duration<=10800 THEN 1 ELSE 0 END) AS channel_middle_prefer_cctv4,
    (CASE WHEN e.deviceid IS NOT NULL AND d.play_duration>10800 THEN 1 ELSE 0 END) AS channel_high_prefer_cctv4,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_cctv5,
    (CASE WHEN f.deviceid IS NOT NULL AND f.play_duration<3600 THEN 1 ELSE 0 END) AS channel_low_prefer_cctv5,
    (CASE WHEN f.deviceid IS NOT NULL AND f.play_duration>=3600 AND f.play_duration<=10800 THEN 1 ELSE 0 END) AS channel_middle_prefer_cctv5,
    (CASE WHEN f.deviceid IS NOT NULL AND f.play_duration>10800 THEN 1 ELSE 0 END) AS channel_high_prefer_cctv5,
    (CASE WHEN g.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_cctv6,
    (CASE WHEN g.deviceid IS NOT NULL AND g.play_duration<3600 THEN 1 ELSE 0 END) AS channel_low_prefer_cctv6,
    (CASE WHEN g.deviceid IS NOT NULL AND g.play_duration>=3600 AND g.play_duration<=10800 THEN 1 ELSE 0 END) AS channel_middle_prefer_cctv6,
    (CASE WHEN g.deviceid IS NOT NULL AND g.play_duration>10800 THEN 1 ELSE 0 END) AS channel_high_prefer_cctv6,
    (CASE WHEN h.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_cctv7,
    (CASE WHEN h.deviceid IS NOT NULL AND h.play_duration<3600 THEN 1 ELSE 0 END) AS channel_low_prefer_cctv7,
    (CASE WHEN h.deviceid IS NOT NULL AND h.play_duration>=3600 AND h.play_duration<=10800 THEN 1 ELSE 0 END) AS channel_middle_prefer_cctv7,
    (CASE WHEN h.deviceid IS NOT NULL AND h.play_duration>10800 THEN 1 ELSE 0 END) AS channel_high_prefer_cctv7,
    (CASE WHEN i.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_cctv8,
    (CASE WHEN i.deviceid IS NOT NULL AND i.play_duration<3600 THEN 1 ELSE 0 END) AS channel_low_prefer_cctv8,
    (CASE WHEN i.deviceid IS NOT NULL AND i.play_duration>=3600 AND i.play_duration<=10800 THEN 1 ELSE 0 END) AS channel_middle_prefer_cctv8,
    (CASE WHEN i.deviceid IS NOT NULL AND i.play_duration>10800 THEN 1 ELSE 0 END) AS channel_high_prefer_cctv8,
    (CASE WHEN j.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_cctv9,
    (CASE WHEN j.deviceid IS NOT NULL AND j.play_duration<3600 THEN 1 ELSE 0 END) AS channel_low_prefer_cctv9,
    (CASE WHEN j.deviceid IS NOT NULL AND j.play_duration>=3600 AND j.play_duration<=10800 THEN 1 ELSE 0 END) AS channel_middle_prefer_cctv9,
    (CASE WHEN j.deviceid IS NOT NULL AND j.play_duration>10800 THEN 1 ELSE 0 END) AS channel_high_prefer_cctv9,
    (CASE WHEN k.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_cctv10,
    (CASE WHEN k.deviceid IS NOT NULL AND k.play_duration<3600 THEN 1 ELSE 0 END) AS channel_low_prefer_cctv10,
    (CASE WHEN k.deviceid IS NOT NULL AND k.play_duration>=3600 AND k.play_duration<=10800 THEN 1 ELSE 0 END) AS channel_middle_prefer_cctv10,
    (CASE WHEN k.deviceid IS NOT NULL AND k.play_duration>10800 THEN 1 ELSE 0 END) AS channel_high_prefer_cctv10,
    (CASE WHEN l.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_cctv11,
    (CASE WHEN l.deviceid IS NOT NULL AND l.play_duration<3600 THEN 1 ELSE 0 END) AS channel_low_prefer_cctv11,
    (CASE WHEN l.deviceid IS NOT NULL AND l.play_duration>=3600 AND l.play_duration<=10800 THEN 1 ELSE 0 END) AS channel_middle_prefer_cctv11,
    (CASE WHEN l.deviceid IS NOT NULL AND l.play_duration>10800 THEN 1 ELSE 0 END) AS channel_high_prefer_cctv11,
    (CASE WHEN m.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_cctv12,
    (CASE WHEN m.deviceid IS NOT NULL AND m.play_duration<3600 THEN 1 ELSE 0 END) AS channel_low_prefer_cctv12,
    (CASE WHEN m.deviceid IS NOT NULL AND m.play_duration>=3600 AND m.play_duration<=10800 THEN 1 ELSE 0 END) AS channel_middle_prefer_cctv12,
    (CASE WHEN m.deviceid IS NOT NULL AND m.play_duration>10800 THEN 1 ELSE 0 END) AS channel_high_prefer_cctv12,
    (CASE WHEN n.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_cctv13,
    (CASE WHEN n.deviceid IS NOT NULL AND n.play_duration<3600 THEN 1 ELSE 0 END) AS channel_low_prefer_cctv13,
    (CASE WHEN n.deviceid IS NOT NULL AND n.play_duration>=3600 AND n.play_duration<=10800 THEN 1 ELSE 0 END) AS channel_middle_prefer_cctv13,
    (CASE WHEN n.deviceid IS NOT NULL AND n.play_duration>10800 THEN 1 ELSE 0 END) AS channel_high_prefer_cctv13,
    (CASE WHEN o.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_cctv14,
    (CASE WHEN o.deviceid IS NOT NULL AND o.play_duration<3600 THEN 1 ELSE 0 END) AS channel_low_prefer_cctv14,
    (CASE WHEN o.deviceid IS NOT NULL AND o.play_duration>=3600 AND o.play_duration<=10800 THEN 1 ELSE 0 END) AS channel_middle_prefer_cctv14,
    (CASE WHEN o.deviceid IS NOT NULL AND o.play_duration>10800 THEN 1 ELSE 0 END) AS channel_high_prefer_cctv14,
    (CASE WHEN p.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_cctv15,
    (CASE WHEN p.deviceid IS NOT NULL AND p.play_duration<3600 THEN 1 ELSE 0 END) AS channel_low_prefer_cctv15,
    (CASE WHEN p.deviceid IS NOT NULL AND p.play_duration>=3600 AND p.play_duration<=10800 THEN 1 ELSE 0 END) AS channel_middle_prefer_cctv15,
    (CASE WHEN p.deviceid IS NOT NULL AND p.play_duration>10800 THEN 1 ELSE 0 END) AS channel_high_prefer_cctv15,
    (CASE WHEN q.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_cctv16,
    (CASE WHEN q.deviceid IS NOT NULL AND q.play_duration<3600 THEN 1 ELSE 0 END) AS channel_low_prefer_cctv16,
    (CASE WHEN q.deviceid IS NOT NULL AND q.play_duration>=3600 AND o.play_duration<=10800 THEN 1 ELSE 0 END) AS channel_middle_prefer_cctv16,
    (CASE WHEN q.deviceid IS NOT NULL AND q.play_duration>10800 THEN 1 ELSE 0 END) AS channel_high_prefer_cctv16,
    (CASE WHEN r.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_cctv17,
    (CASE WHEN r.deviceid IS NOT NULL AND r.play_duration<3600 THEN 1 ELSE 0 END) AS channel_low_prefer_cctv17,
    (CASE WHEN r.deviceid IS NOT NULL AND r.play_duration>=3600 AND o.play_duration<=10800 THEN 1 ELSE 0 END) AS channel_middle_prefer_cctv17,
    (CASE WHEN r.deviceid IS NOT NULL AND r.play_duration>10800 THEN 1 ELSE 0 END) AS channel_high_prefer_cctv17 
FROM(
    SELECT deviceid AS key FROM knowyou_ott_edw.edw_mbh_deviceinfo
    WHERE date_time='20220615'
    GROUP BY deviceid
)a
LEFT JOIN(
    SELECT deviceid,play_duration FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='CCTV1'
)b ON a.key=b.deviceid
LEFT JOIN(
    SELECT deviceid,play_duration FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='CCTV2'
)c ON a.key=c.deviceid
LEFT JOIN(
    SELECT deviceid,play_duration FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='CCTV3'
)d ON a.key=d.deviceid
LEFT JOIN(
    SELECT deviceid,play_duration FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='CCTV4'
)e ON a.key=e.deviceid
LEFT JOIN(
    SELECT deviceid,play_duration FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='CCTV5'
)f ON a.key=f.deviceid
LEFT JOIN(
    SELECT deviceid,play_duration FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='CCTV6'
)g ON a.key=g.deviceid
LEFT JOIN(
    SELECT deviceid,play_duration FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='CCTV7'
)h ON a.key=h.deviceid
LEFT JOIN(
    SELECT deviceid,play_duration FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='CCTV8'
)i ON a.key=i.deviceid
LEFT JOIN(
    SELECT deviceid,play_duration FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='CCTV9'
)j ON a.key=j.deviceid
LEFT JOIN(
    SELECT deviceid,play_duration FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='CCTV10'
)k ON a.key=k.deviceid
LEFT JOIN(
    SELECT deviceid,play_duration FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='CCTV11'
)l ON a.key=l.deviceid
LEFT JOIN(
    SELECT deviceid,play_duration FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='CCTV12'
)m ON a.key=m.deviceid
LEFT JOIN(
    SELECT deviceid,play_duration FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='CCTV13'
)n ON a.key=n.deviceid
LEFT JOIN(
    SELECT deviceid,play_duration FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='CCTV14'
)o ON a.key=o.deviceid
LEFT JOIN(
    SELECT deviceid,play_duration FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='CCTV15'
)p ON a.key=p.deviceid 
LEFT JOIN(
    SELECT deviceid,play_duration FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='CCTV16'
)q ON a.key=q.deviceid 
LEFT JOIN(
    SELECT deviceid,play_duration FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='CCTV17'
)r ON a.key=r.deviceid



--6 当日直播卫视频道偏好标签表
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_channel_prefer_dm PARTITION(date_time)
SELECT 
    a.key AS deviceid,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv1, -- 江苏卫视 
    (CASE WHEN c.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv2,
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv3,
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv4,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv5,
    (CASE WHEN g.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv6,
    (CASE WHEN h.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv7,
    (CASE WHEN i.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv8,
    (CASE WHEN j.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv9,
    (CASE WHEN k.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv10,
    (CASE WHEN l.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv11,
    (CASE WHEN m.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv12,
    (CASE WHEN n.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv13,
    (CASE WHEN o.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv14,
    (CASE WHEN p.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv15,
    (CASE WHEN q.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv16,
    (CASE WHEN r.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv17,
    (CASE WHEN s.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv18,
    (CASE WHEN t.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv19,
    (CASE WHEN u.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv20,
    (CASE WHEN v.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv21,
    (CASE WHEN w.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv22,
    (CASE WHEN x.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv23,
    (CASE WHEN y.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS channel_prefer_lv24,
    '20220615' AS date_time
FROM(
    SELECT deviceid AS key FROM knowyou_ott_edw.edw_mbh_deviceinfo
    WHERE date_time='20220615'
    GROUP BY deviceid
)a
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='江苏卫视'
)b ON a.key=b.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='浙江卫视'
)c ON a.key=c.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='湖南卫视'
)d ON a.key=d.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='北京卫视'
)e ON a.key=e.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='深圳卫视'
)f ON a.key=f.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='辽宁卫视'
)g ON a.key=g.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='湖北卫视'
)h ON a.key=h.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='安徽卫视'
)i ON a.key=i.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='重庆卫视'
)j ON a.key=j.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='天津卫视'
)k ON a.key=k.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='黑龙江卫视'
)l ON a.key=l.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='吉林卫视'
)m ON a.key=m.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='内蒙古卫视'
)n ON a.key=n.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='河北卫视'
)o ON a.key=o.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='河南卫视'
)p ON a.key=p.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='江西卫视'
)q ON a.key=q.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='广东卫视'
)r ON a.key=r.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='广西卫视'
)s ON a.key=s.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='东南卫视'
)t ON a.key=t.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='云南卫视'
)u ON a.key=u.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='贵州卫视'
)v ON a.key=v.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='山西卫视'
)w ON a.key=w.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='山东卫视'
)x ON a.key=x.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_dmt.htv_channel_dm
    WHERE date_time='20220615' AND channelname='宁夏卫视'
)y ON a.key=y.deviceid


-- 7.当日点播栏目偏好标签表
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_demand_column_prefer_dm PARTITION(date_time)
SELECT  
    a.key AS deviceid,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv1,
    (CASE WHEN c.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv2,
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv3,
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv4,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv5,
    (CASE WHEN g.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv6,
    (CASE WHEN h.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv7,
    (CASE WHEN i.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv8,
    (CASE WHEN j.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv9,
    (CASE WHEN k.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv10,
    (CASE WHEN l.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv11,
    (CASE WHEN m.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_other,
    '20220615' AS date_time
FROM(
    SELECT deviceid AS key FROM knowyou_ott_edw.edw_mbh_deviceinfo
    WHERE date_time='20220615'
    GROUP BY deviceid
)a
LEFT JOIN( 
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='电视剧' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)b ON a.key=b.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='电影' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)c ON a.key=c.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='动漫' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)d ON a.key=d.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='体育' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)e ON a.key=e.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND (contentType='纪录片' OR contentType='纪实') AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)f ON a.key=f.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='教育' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)g ON a.key=g.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='游戏' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)h ON a.key=h.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='电竞' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)i ON a.key=i.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='综艺' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)j ON a.key=j.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='生活' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)k ON a.key=k.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='少儿' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)l ON a.key=l.deviceid 
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType!='少儿' AND contentType!='生活' AND contentType!='综艺' 
    AND  contentType!='游戏' AND contentType!='电竞' AND contentType!='教育' AND contentType!='纪录片' 
    AND contentType!='体育' AND contentType!='动漫' AND contentType!='电影' AND  contentType!='电视剧' 
    AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)m ON a.key=m.deviceid


-- 当日点播类别偏好标签表
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_demand_type_prefer_dm PARTITION(date_time)
SELECT  
    a.key AS deviceid,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv1,
    (CASE WHEN c.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv2,
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv3,
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv4,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv5,
    (CASE WHEN g.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv6,
    (CASE WHEN h.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv7,
    (CASE WHEN i.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv8,
    (CASE WHEN j.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv9,
    (CASE WHEN k.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv10,
    (CASE WHEN l.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv11,
    (CASE WHEN m.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv12,
    (CASE WHEN n.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv13,
    (CASE WHEN o.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv14,
    (CASE WHEN p.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv15,
    (CASE WHEN q.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv16,
    (CASE WHEN r.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv17,
    '20220615' AS date_time
FROM(
    SELECT deviceid AS key FROM knowyou_ott_edw.edw_mbh_deviceinfo
    WHERE date_time='20220615'
    GROUP BY deviceid
)a
LEFT JOIN( 
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='电视剧' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)b ON a.key=b.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='电影' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)c ON a.key=c.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='综艺' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)d ON a.key=d.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='少儿' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)e ON a.key=e.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='网剧' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)f ON a.key=f.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='微电影' AND playstatus='开始' AND videotype='点播'
)g ON a.key=g.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='短视频' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)h ON a.key=h.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='动漫' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)i ON a.key=i.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='体育' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)j ON a.key=j.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='纪录片' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)k ON a.key=k.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='教育' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)l ON a.key=l.deviceid 
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='电竞' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)m ON a.key=m.deviceid 
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='游戏' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)n ON a.key=n.deviceid 
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='音乐' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)o ON a.key=o.deviceid 
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='娱乐' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)p ON a.key=p.deviceid 
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='戏曲' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)q ON a.key=q.deviceid 
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND contentType='生活' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)r ON a.key=r.deviceid 



-- 7.当月点播栏目偏好标签表
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_demand_type_prefer_dm_month PARTITION(date_time)
SELECT  
    a.key AS deviceid,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv1,
    (CASE WHEN c.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv2,
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv3,
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv4,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv5,
    (CASE WHEN g.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv6,
    (CASE WHEN h.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv7,
    (CASE WHEN i.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv8,
    (CASE WHEN j.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv9,
    (CASE WHEN k.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv10,
    (CASE WHEN l.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_lv11,
    (CASE WHEN m.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demand_type_prefer_other,
    '20220615' AS date_time
FROM(
    SELECT deviceid AS key FROM knowyou_ott_edw.edw_mbh_deviceinfo
    WHERE date_time<='20220615' AND date_time>='20220516'
    GROUP BY deviceid
)a
LEFT JOIN( 
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND contentType='电视剧' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)b ON a.key=b.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND contentType='电影' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)c ON a.key=c.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND contentType='动漫' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)d ON a.key=d.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND contentType='体育' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)e ON a.key=e.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND contentType='纪录片' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)f ON a.key=f.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND contentType='教育' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)g ON a.key=g.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND contentType='游戏' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)h ON a.key=h.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND contentType='电竞' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)i ON a.key=i.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND contentType='综艺' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)j ON a.key=j.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND contentType='生活' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)k ON a.key=k.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND contentType='少儿' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)l ON a.key=l.deviceid 
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time<='20220615' AND date_time>='20220516' AND contentType!='少儿' AND contentType!='生活' AND contentType!='综艺' 
    AND  contentType!='游戏' AND contentType!='电竞' AND contentType!='教育' AND contentType!='纪录片' AND contentType!='体育' 
    AND contentType!='动漫' AND contentType!='电影' AND  contentType!='电视剧' AND playstatus='开始' AND videotype='点播'
    GROUP BY deviceid
)m ON a.key=m.deviceid






-------8 行为标签 新增留存分类 
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_adduserretentionlabe_dm PARTITION(date_time='20220615')
SELECT 
    deviceid,
    max(CASE WHEN date_time ='20220614' AND day_1 IS NOT NULL THEN 1 ELSE 0 END ) AS morrow_adduser,
    max(CASE WHEN date_time ='20220608' AND day_7 IS NOT NULL THEN 1 ELSE 0 END ) AS seven_adduser,
    max(CASE WHEN date_time ='20220601' AND day_14 IS NOT NULL THEN 1 ELSE 0 END ) AS fourteen_adduser,
    max(CASE WHEN date_time ='20220516' AND day_30 IS NOT NULL THEN 1 ELSE 0 END ) AS thirty_adduser
FROM
(
    SELECT 
    deviceid,
    cast(MAX(CASE WHEN d.date_time=date_format(date_add(from_unixtime(unix_timestamp(d.dt,'yyyyMMdd'),'yyyy-MM-dd'),1),'yyyyMMdd') THEN retention_usernum END) AS int) AS day_1,
    cast(MAX(CASE WHEN d.date_time=date_format(date_add(from_unixtime(unix_timestamp(d.dt,'yyyyMMdd'),'yyyy-MM-dd'),7),'yyyyMMdd') THEN retention_usernum END) AS int) AS day_7,
    cast(MAX(CASE WHEN d.date_time=date_format(date_add(from_unixtime(unix_timestamp(d.dt,'yyyyMMdd'),'yyyy-MM-dd'),14),'yyyyMMdd') THEN retention_usernum END) AS int) AS day_14,
    cast(MAX(CASE WHEN d.date_time=date_format(date_add(from_unixtime(unix_timestamp(d.dt,'yyyyMMdd'),'yyyy-MM-dd'),29),'yyyyMMdd') THEN retention_usernum END) AS int) AS day_30,
    d.dt AS date_time
    FROM
    (
        SELECT 
            c.dt,
            c.deviceid,
            c.date_time,
            SUM(c.open_state) AS retention_usernum
        FROM
        (
            SELECT a.deviceid, substr(a.createtime,1,8) AS dt, b.date_time, b.open_state FROM knowyou_ott_edw.edw_mbh_deviceinfo a
            LEFT JOIN
            (
                SELECT deviceid, date_time AS date_time, '1' AS open_state FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
                WHERE date_time >='20220516' AND date_time<='20220615'
                GROUP BY date_time, deviceid
            )b ON a.deviceid=b.deviceid
            WHERE substr(a.createtime,1,8) >='20220516' AND substr(a.createtime,1,8)<='20220615'
        )c
        GROUP BY c.dt, c.date_time, c.deviceid
    )d
    GROUP BY d.dt, d.deviceid
)ff
GROUP BY deviceid, date_time




--------- 9 行为标签 活跃留存分类 
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_activeretentionlabe_dm PARTITION(date_time='20220615')
SELECT 
    deviceid,
    (CASE WHEN day_1=2 THEN 1 ELSE 0 END) AS morrow_activer,
    (CASE WHEN day_7=7 THEN 1 ELSE 0 END) AS seven_activer,
    (CASE WHEN day_14=14 THEN 1 ELSE 0 END) AS fourteen_activer,
    (CASE WHEN day_30=30 THEN 1 ELSE 0 END) AS thirty_activer
    FROM
(
    SELECT
        deviceid,
        sum(CASE WHEN date_time >='20220614' THEN 1 ELSE 0 END ) AS day_1,
        sum(CASE WHEN date_time >='20220608' AND date_time <='20220615' THEN 1 ELSE 0 END ) AS day_7,
        sum(CASE WHEN date_time >='20220601' AND date_time <='20220615' THEN 1 ELSE 0 END ) AS day_14,
        sum(CASE WHEN date_time >='20220516' AND date_time <='20220615' THEN 1 ELSE 0 END ) AS day_30
    FROM
    (
        SELECT date_time, deviceid, '1' AS activer_user FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time >'20220516'
        GROUP BY date_time,deviceid
    )a
    GROUP BY deviceid
)ff



---------------10 行为标签 月/活跃/新增留存分类
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_activeadd_retention_mm PARTITION(date_time='20220615')
SELECT 
    '20220615' AS deal_time,
    aa.deviceid,
    aa.month_activer,
    (CASE WHEN bb.month_adduer IS NULL THEN 0 ELSE bb.month_adduer END) AS month_adduer
FROM
(
    SELECT
        a.deviceid,
        (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_activer
    FROM
    (
        SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE substr(date_time,1,6) = '202205'
        GROUP BY deviceid
    )a
    LEFT JOIN
    (
        SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615'
        GROUP BY deviceid
    )b ON a.deviceid = b.deviceid 
)aa
LEFT JOIN 
(
    SELECT
        a.deviceid,
        (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_adduer
    FROM
    (
        SELECT deviceid FROM knowyou_ott_edw.edw_mbh_deviceinfo
        WHERE substr(createtime,1,6) >='202205'
        GROUP BY deviceid
    )a
    LEFT JOIN
    (
        SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615'
        GROUP BY deviceid
    )b ON a.deviceid=b.deviceid
)bb
ON aa.deviceid =bb.deviceid



-- 15.7日回流用户
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_back_flow_lv7 PARTITION(date_time)
SELECT tt.* FROM 
(
    SELECT 
        (CASE WHEN b.deviceid IS NULL AND c.deviceid IS NOT NULL THEN a.deviceid ELSE null END) AS deviceid,
        '20220615' AS date_time -- 7日回流用户
    FROM
    (
        SELECT deviceid FROM knowyou_ott_edw.edw_mbh_deviceinfo
        WHERE date_time='20220615'
        GROUP BY deviceid
    )a -- 前两天全量用户
    LEFT JOIN
    (
        SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time>='20220608' AND date_time<='20220614'
        GROUP BY deviceid
    )b -- 前七天开机用户
    ON a.deviceid=b.deviceid
    LEFT JOIN
    (
        SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615'
        GROUP BY deviceid
    )c -- 昨日开机用户
    ON a.deviceid=c.deviceid
)tt 
WHERE tt.deviceid IS NOT NULL



-- 15.14日回流用户
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_back_flow_lv14 PARTITION(date_time)
SELECT tt.* FROM 
(
    SELECT 
        (CASE WHEN b.deviceid IS NULL AND c.deviceid IS NOT NULL THEN a.deviceid ELSE null END) AS deviceid,
        '20220615' AS date_time -- 14日回流用户
    FROM(
        SELECT deviceid FROM knowyou_ott_edw.edw_mbh_deviceinfo
        WHERE date_time='20220615'
        GROUP BY deviceid
    )a -- 前两天全量用户
    LEFT JOIN(
        SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time>='20220520' AND date_time<='20220614'
        GROUP BY deviceid
    )b -- 前14天开机用户
    ON a.deviceid=b.deviceid
    LEFT JOIN(
        SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615'
        GROUP BY deviceid
    )c -- 昨日开机用户
    ON a.deviceid=c.deviceid
)tt 
WHERE tt.deviceid IS NOT NULL




-- 15.30日回流用户
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_back_flow_lv30 PARTITION(date_time)
SELECT tt.* FROM (
    SELECT 
        (CASE WHEN b.deviceid IS NULL AND c.deviceid IS NOT NULL THEN a.deviceid ELSE null END) AS deviceid,
        '20220615' AS date_time -- 30日回流用户
    FROM(
        SELECT deviceid FROM knowyou_ott_edw.edw_mbh_deviceinfo
        WHERE date_time='20220615'
        GROUP BY deviceid
    )a -- 前两天全量用户
    LEFT JOIN(
        SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time>='20220516' AND date_time<='20220614'
        GROUP BY deviceid
    )b -- 前30天开机用户
    ON a.deviceid=b.deviceid
    LEFT JOIN(
        SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615'
        GROUP BY deviceid
    )c -- 昨日开机用户
    ON a.deviceid=c.deviceid
)tt 
WHERE tt.deviceid IS NOT NULL



-- 沉默用户
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_slience_dm PARTITION(date_time)
SELECT  
    a.deviceid,
    (CASE WHEN b.deviceid IS NULL THEN 1 ELSE 0 END) AS pre7_slience_user,
    (CASE WHEN c.deviceid IS NULL THEN 1 ELSE 0 END) AS pre14_slience_user,
    (CASE WHEN d.deviceid IS NULL THEN 1 ELSE 0 END) AS pre30_slience_user,
    (CASE WHEN e.deviceid IS NULL THEN 1 ELSE 0 END) AS pre_mon_slience_user,
    '20220615' AS date_time
FROM(
    SELECT deviceid FROM knowyou_ott_edw.edw_mbh_deviceinfo
    WHERE date_time='20220615'
    GROUP BY deviceid
)a
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time>='20220608' AND date_time<='20220614'
    GROUP BY deviceid
)b ON a.deviceid = b.deviceid 
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time>='20220601' AND date_time<='20220615'
    GROUP BY deviceid
)c ON a.deviceid = c.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time>='20220516' AND date_time<='20220615'
    GROUP BY deviceid
)d ON a.deviceid = d.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time>='20220416' AND date_time<='20220516'
    GROUP BY deviceid
)e ON a.deviceid = e.deviceid;




-- 直播内容轻偏好
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_live_content_light PARTITION(date_time)
SELECT 
    t0.deviceid,
    t1.videoname,
    '20220615' AS date_time
FROM
(
    SELECT a.deviceid, a.videoname
    FROM
    (
        SELECT deviceid, programname AS videoname, sum(playTime) AS play_duration 
        FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615' AND programname !='' AND playstatus='开始' AND videotype='直播' 
        GROUP BY deviceid, programname
    )a WHERE a.play_duration<1800
)t0
LEFT JOIN
(
    SELECT videoname FROM 
    (
        SELECT videoname, row_number() over(PARTITION by secondlevel ORDER BY playnum desc) ranks 
        FROM knowyou_ott_dmt.dmt_ht_secondlevelvideoname_wb
        WHERE date_time='20220615'
    )b 
    WHERE b.ranks<=100 
    GROUP BY videoname
)t1 ON t0.videoname=t1.videoname
WHERE t1.videoname IS NOT NULL




-- 直播内容100个中偏好
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_live_content_middle PARTITION(date_time)
SELECT  
    t0.deviceid,
    t1.videoname, 
    '20220615' AS date_time
FROM
(
    SELECT a.deviceid, a.videoname
    FROM
    (
        SELECT deviceid, programname videoname, sum(playTime) AS play_duration 
        FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615' AND programname !='' AND playstatus='开始' AND videotype='直播' 
        GROUP BY deviceid, programname
    )a 
    WHERE a.play_duration>=1800 AND a.play_duration<=3600
)t0
LEFT JOIN
(
    SELECT videoname FROM 
    (
        SELECT videoname, row_number() over(PARTITION by secondlevel ORDER BY playnum desc)ranks 
        FROM knowyou_ott_dmt.dmt_ht_secondlevelvideoname_wb
        WHERE date_time='20220615'
    )b 
    WHERE b.ranks<=100
    GROUP BY videoname
)t1 ON t0.videoname=t1.videoname
WHERE t1.videoname IS NOT NULL




-- 直播内容100个重偏好
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_live_content_high PARTITION(date_time)
SELECT  
    t0.deviceid,
    t1.videoname, 
    '20220615' AS date_time
FROM
(
    SELECT a.deviceid, a.videoname FROM
    (
        SELECT deviceid, programname videoname, sum(playTime) AS play_duration 
        FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615' AND programname !='' AND playstatus='开始' AND videotype='直播' 
        GROUP BY deviceid, programname
    )a 
    WHERE a.play_duration>3600
)t0
LEFT JOIN
(
    SELECT distinct videoname FROM 
    (
        SELECT videoname, row_number() over(PARTITION by secondlevel ORDER BY playnum desc)ranks 
        FROM knowyou_ott_dmt.dmt_ht_secondlevelvideoname_wb
        WHERE date_time='20220615'
    )b 
    WHERE b.ranks<=100
)t1 ON t0.videoname=t1.videoname
WHERE t1.videoname IS NOT NULL




-- 点播内容100个轻偏好
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_demand_content_light PARTITION(date_time)
SELECT 
    t0.deviceid,
    t1.videoname, 
    '20220615' AS date_time
FROM
(
    SELECT a.deviceid, a.videoname FROM
    (
        SELECT deviceid, programname AS videoname, sum(playTime) AS play_duration 
        FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615' AND programname !='' AND playstatus='开始' AND videotype='点播' 
        GROUP BY deviceid, programname
    )a 
    WHERE a.play_duration<3600
)t0
LEFT JOIN
(
    SELECT distinct videoname FROM 
    (
        SELECT videoname, row_number() over(PARTITION by secondlevel ORDER BY playnum desc)ranks 
        FROM knowyou_ott_dmt.dmt_ht_secondlevelvideoname_wb
        WHERE date_time='20220615'
    )b 
    WHERE b.ranks<=100 
)t1 ON t0.videoname=t1.videoname
WHERE t1.videoname IS NOT NULL



-- 点播内容100个中偏好
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_demand_content_middle PARTITION(date_time)
SELECT 
    t0.deviceid,
    t1.videoname, 
    '20220615' AS date_time
FROM
(
    SELECT a.deviceid, a.videoname FROM
    (
        SELECT deviceid, programname videoname, sum(playTime) AS play_duration 
        FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615' AND programname !='' AND playstatus='开始' AND videotype='点播' 
        GROUP BY deviceid,programname
    )a 
    WHERE a.play_duration>=3600 AND a.play_duration<=10800
)t0
LEFT JOIN
(
    SELECT distinct videoname FROM 
    (
        SELECT videoname, row_number() over(PARTITION by secondlevel ORDER BY playnum desc)ranks 
        FROM knowyou_ott_dmt.dmt_ht_secondlevelvideoname_wb
        WHERE date_time='20220615'
    )b WHERE b.ranks<=100
)t1 ON t0.videoname=t1.videoname
WHERE t1.videoname IS NOT NULL




-- 点播内容100个重偏好
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_demand_content_high PARTITION(date_time)
SELECT  
    t0.deviceid,
    t1.videoname, 
    '20220615' AS date_time
FROM
(
    SELECT a.deviceid, a.videoname FROM
    (
        SELECT deviceid, programname videoname, sum(playTime) AS play_duration 
        FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615' AND programname !='' AND playstatus='开始' AND videotype='点播' 
        GROUP BY deviceid, programname
    )a 
    WHERE a.play_duration>10800
)t0
LEFT JOIN
(
    SELECT distinct videoname FROM 
    (
        SELECT videoname, row_number() over(PARTITION by secondlevel ORDER BY playnum desc)ranks 
        FROM knowyou_ott_dmt.dmt_ht_secondlevelvideoname_wb
        WHERE date_time='20220615'
    )b 
    WHERE b.ranks<=100
)t1 ON t0.videoname=t1.videoname
WHERE t1.videoname IS NOT NULL



--------- 行为标签 搜索收藏分类
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_searchcollectlabe_dm PARTITION(date_time='20220615')
SELECT 
    aa.tag,
    aa.videoname,
    aa.playnum AS playnums
FROM
(
    SELECT * FROM
    (
        SELECT 't1' AS tag, programname AS videoname, count(*) AS playnum 
        FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE actionpath regexp('.*搜索.*') AND videotype='点播' AND date_time='20220615' AND programname<>"" 
        GROUP BY programname
        ORDER BY playnum desc
        limit 10
    )ff
    UNION ALL
    SELECT * FROM
    (
        SELECT 't2' AS tag, programname AS videoname, count(*) AS playnum 
        FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE actionpath regexp('.*搜索.*') AND videotype='点播' AND substr(date_time,1,6)='202206' AND programname<>"" 
        GROUP BY programname
        ORDER BY playnum desc
        limit 10
    )ff
    UNION ALL
    SELECT * FROM
    (
        SELECT 't3' AS tag, programname AS videoname, count(*) AS playnum 
        FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer 
        WHERE actionpath regexp('.*收藏.*') AND videotype='点播' AND date_time='20220615' AND programname<>"" 
        GROUP BY programname
        ORDER BY playnum desc
        limit 10
    )ff
    UNION ALL
    SELECT * FROM
    (
        SELECT 't4' AS tag, programname AS videoname, count(*) AS playnum 
        FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer 
        WHERE actionpath regexp('.*收藏.*') AND videotype='点播' AND substr(date_time,1,6)='202206' AND programname<>"" 
        GROUP BY programname
        ORDER BY playnum desc
        limit 10
    )ff
)aa



-- 当日收看各类APP信息情况表
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_app_basic PARTITION(date_time)
SELECT 
    a.deviceid,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS jiguang_pre,    --当日收看极光TVAPP
    (CASE WHEN c.deviceid IS NOT NULL THEN c.play_num ELSE 0 END) AS jiguang_count,  --当日收看极光TVAPP的次数
    (CASE WHEN c.deviceid IS NOT NULL THEN c.play_duration ELSE 0 END) AS jiguang_time, --当日收看极光TVAPP的时长
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS qiyi_pre,
    (CASE WHEN e.deviceid IS NOT NULL THEN e.play_num ELSE 0 END) AS qiyi_count,
    (CASE WHEN e.deviceid IS NOT NULL THEN e.play_duration ELSE 0 END) AS qiyi_time,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS bailin_pre,
    (CASE WHEN g.deviceid IS NOT NULL THEN g.play_num ELSE 0 END) AS bailin_count,
    (CASE WHEN g.deviceid IS NOT NULL THEN g.play_duration ELSE 0 END) AS bailin_time,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS other_pre,
    (CASE WHEN g.deviceid IS NOT NULL THEN g.play_num ELSE 0 END) AS other_count,
    (CASE WHEN g.deviceid IS NOT NULL THEN g.play_duration ELSE 0 END) AS other_time,
    '20220615' AS date_time 
FROM 
(
    SELECT deviceid FROM knowyou_ott_edw.edw_mbh_deviceinfo
    WHERE date_time='20220615'
    GROUP BY deviceid
)a
LEFT JOIN 
(
    SELECT deviceid, appname FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer 
    WHERE date_time=20220615 AND appname='tv.huan.tencentTV'
)b ON a.deviceid=b.deviceid  
LEFT JOIN 
(
    SELECT deviceid, count(*) AS play_num, round(sum(playTime)/3600,1) AS play_duration
    FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND appname='tv.huan.tencentTV'
    GROUP BY deviceid 
)c ON a.deviceid=c.deviceid 
LEFT JOIN 
(
    SELECT deviceid, appname FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer 
    WHERE date_time=20220615 AND appname='com.gitvvideo.sichuanyidong' 
)d ON a.deviceid=d.deviceid  
LEFT JOIN 
(
    SELECT deviceid, count(*) AS play_num, round(sum(playTime)/3600,1) AS play_duration
    FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND appname='com.gitvvideo.sichuanyidong' 
    GROUP BY deviceid 
)e ON a.deviceid=e.deviceid 
LEFT JOIN 
(
    SELECT deviceid, appname FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer 
    WHERE date_time=20220615 AND appname='tv.icntv.ott' 
)f ON a.deviceid=f.deviceid  
LEFT JOIN 
(
    SELECT deviceid, count(*) AS play_num, round(sum(playTime)/3600,1) AS play_duration
    FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND appname='tv.icntv.ott' 
    GROUP BY deviceid 
)g ON a.deviceid=g.deviceid 
LEFT JOIN 
(
    SELECT deviceid, appname FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer 
    WHERE date_time=20220615 AND appname!='tv.icntv.ott' AND appname!='com.gitvvideo.sichuanyidong' AND appname!='tv.huan.tencentTV' 
)h ON a.deviceid=h.deviceid  
LEFT JOIN 
(
    SELECT deviceid, count(*) AS play_num, round(sum(playTime)/3600,1) AS play_duration
    FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND appname!='tv.icntv.ott' AND appname!='com.gitvvideo.sichuanyidong' AND appname!='tv.huan.tencentTV'  
    GROUP BY deviceid 
)i ON a.deviceid=i.deviceid ;



-- 当月收看各类APP信息情况表
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_app_basic_month PARTITION(date_time)
SELECT 
    a.deviceid,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS jiguang_pre,    --当月收看极光TVAPP
    (CASE WHEN c.deviceid IS NOT NULL THEN c.play_num ELSE 0 END) AS jiguang_count,  --当月收看极光TVAPP的次数
    (CASE WHEN c.deviceid IS NOT NULL THEN c.play_duration ELSE 0 END) AS jiguang_time, --当月收看极光TVAPP的时长
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS qiyi_pre,
    (CASE WHEN e.deviceid IS NOT NULL THEN e.play_num ELSE 0 END) AS qiyi_count,
    (CASE WHEN e.deviceid IS NOT NULL THEN e.play_duration ELSE 0 END) AS qiyi_time,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS bailin_pre,
    (CASE WHEN g.deviceid IS NOT NULL THEN g.play_num ELSE 0 END) AS bailin_count,
    (CASE WHEN g.deviceid IS NOT NULL THEN g.play_duration ELSE 0 END) AS bailin_time,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS other_pre,
    (CASE WHEN g.deviceid IS NOT NULL THEN g.play_num ELSE 0 END) AS other_count,
    (CASE WHEN g.deviceid IS NOT NULL THEN g.play_duration ELSE 0 END) AS other_time,
    '20220615' AS date_time 
FROM  
(
    SELECT deviceid FROM knowyou_ott_edw.edw_mbh_deviceinfo
    WHERE date_time='20220615' AND date_time>=20220516
)a
LEFT JOIN 
(
    SELECT deviceid, appname FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer 
    WHERE date_time<=20220615 AND date_time>=20220516 AND appname='tv.huan.tencentTV'
)b ON a.deviceid=b.deviceid  
LEFT JOIN 
(  
    SELECT deviceid, count(*) AS play_num, round(sum(playTime)/3600,1) AS play_duration
    FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND date_time>=20220516 AND appname='tv.huan.tencentTV'
    GROUP BY deviceid 
)c ON a.deviceid=c.deviceid 
LEFT JOIN 
(
    SELECT deviceid, appname FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer 
    WHERE date_time=20220615 AND date_time>=20220516 AND appname='com.gitvvideo.sichuanyidong' 
)d ON a.deviceid=d.deviceid  
LEFT JOIN 
(
    SELECT deviceid, count(*) AS play_num, round(sum(playTime)/3600,1) AS play_duration
    FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND date_time>=20220516 AND appname='com.gitvvideo.sichuanyidong' 
    GROUP BY deviceid 
)e ON a.deviceid=e.deviceid 
LEFT JOIN 
(
    SELECT deviceid, appname FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer 
    WHERE date_time=20220615 AND date_time>=20220516 AND appname='tv.icntv.ott' 
)f ON a.deviceid=f.deviceid  
LEFT JOIN 
(
    SELECT deviceid, count(*) AS play_num, round(sum(playTime)/3600,1) AS play_duration
    FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND date_time>=20220516 AND appname='tv.icntv.ott' 
    GROUP BY deviceid 
)g ON a.deviceid=g.deviceid 
LEFT JOIN 
(
    SELECT deviceid, appname FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer 
    WHERE date_time=20220615 AND date_time>=20220516 AND appname！='tv.icntv.ott' AND appname!='com.gitvvideo.sichuanyidong' AND appname!='tv.huan.tencentTV' 
)h ON a.deviceid=h.deviceid  
LEFT JOIN 
(
    SELECT deviceid, count(*) AS play_num, round(sum(playTime)/3600,1) AS play_duration
    FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
    WHERE date_time='20220615' AND date_time>=20220516 AND appname！='tv.icntv.ott' AND appname!='com.gitvvideo.sichuanyidong' AND appname!='tv.huan.tencentTV'  
    GROUP BY deviceid 
)i ON a.deviceid=i.deviceid ;




--app收视轻偏好
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_app_light PARTITION(date_time)
SELECT  
    t0.deviceid,
    t0.appname, 
    '20220615' AS date_time
FROM(
    SELECT a.deviceid, a.appname FROM
    (
       SELECT deviceid, appname, sum(appruntime) AS play_duration 
       FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
       WHERE date_time='20220615' AND appname !=''  
       GROUP BY deviceid,appname
    )a 
    WHERE a.play_duration<3600 
)t0




--app收视中偏好
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_app_middle PARTITION(date_time)
SELECT  
    t0.deviceid,
    t0.appname, 
    '20220615' AS date_time
FROM(
    SELECT a.deviceid, a.appname FROM
    (
        SELECT deviceid, appname, sum(appruntime) AS play_duration 
        FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615' AND appname !=''  
        GROUP BY deviceid, appname
    )a 
    WHERE a.play_duration>=3600 AND a.play_duration<=10800 
)t0
 
 
 
 --app收视重偏好
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_app_high PARTITION(date_time)
SELECT  
    t0.deviceid,
    t0.appname, 
    '20220615' AS date_time
FROM(
    SELECT a.deviceid, a.appname FROM
    (
        SELECT deviceid, appname, sum(appruntime) AS play_duration 
        FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615' AND appname !=''  
        GROUP BY deviceid, appname
    )a 
    WHERE a.play_duration>10800 
)t0




-------- 媒资标签 情节分类 标签个数：34
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_plotlabel_dm PARTITION(date_time='20220615')
SELECT 
    deviceid,
    (CASE WHEN programClass like '%喜剧%' THEN 1 ELSE 0 END) AS v_b1,
    (CASE WHEN programClass like '%动作%' THEN 1 ELSE 0 END) AS v_b2,
    (CASE WHEN programClass like '%警匪%' THEN 1 ELSE 0 END) AS v_b3,
    (CASE WHEN programClass like '%爱情%' THEN 1 ELSE 0 END) AS v_b4,
    (CASE WHEN programClass like '%青春%' THEN 1 ELSE 0 END) AS v_b5,
    (CASE WHEN programClass like '%犯罪%' THEN 1 ELSE 0 END) AS v_b6,
    (CASE WHEN programClass like '%悬疑%' THEN 1 ELSE 0 END) AS v_b7,
    (CASE WHEN programClass like '%惊悚%' THEN 1 ELSE 0 END) AS v_b8,
    (CASE WHEN programClass like '%恐怖%' THEN 1 ELSE 0 END) AS v_b9,
    (CASE WHEN programClass like '%灾难%' THEN 1 ELSE 0 END) AS v_b10,
    (CASE WHEN programClass like '%战争%' THEN 1 ELSE 0 END) AS v_b11,
    (CASE WHEN programClass like '%科幻%' THEN 1 ELSE 0 END) AS v_b12,
    (CASE WHEN programClass like '%武侠%' THEN 1 ELSE 0 END) AS v_b13,
    (CASE WHEN programClass like '%纪录片%' THEN 1 ELSE 0 END) AS v_b14,
    (CASE WHEN programClass like '%体育%' THEN 1 ELSE 0 END) AS v_b15,
    (CASE WHEN programClass like '%幼儿%' THEN 1 ELSE 0 END) AS v_b16,
    (CASE WHEN programClass like '%动画%' THEN 1 ELSE 0 END) AS v_b17,
    (CASE WHEN programClass like '%励志%' THEN 1 ELSE 0 END) AS v_b18,
    (CASE WHEN programClass like '%戏曲%' THEN 1 ELSE 0 END) AS v_b19,
    (CASE WHEN programClass like '%广场舞%' THEN 1 ELSE 0 END) AS v_b20,
    (CASE WHEN programClass like '%真人秀%' THEN 1 ELSE 0 END) AS v_b21,
    (CASE WHEN programClass like '%偶像%' THEN 1 ELSE 0 END) AS v_b22,
    (CASE WHEN programClass like '%冒险%' THEN 1 ELSE 0 END) AS v_b23,
    (CASE WHEN programClass like '%时尚%' THEN 1 ELSE 0 END) AS v_b24,
    (CASE WHEN programClass like '%言情%' THEN 1 ELSE 0 END) AS v_b25,
    (CASE WHEN programClass like '%足球%' THEN 1 ELSE 0 END) AS v_b26,
    (CASE WHEN programClass like '%篮球%' THEN 1 ELSE 0 END) AS v_b27,
    (CASE WHEN programClass like '%网球%' THEN 1 ELSE 0 END) AS v_b28,
    (CASE WHEN programClass like '%探索%' THEN 1 ELSE 0 END) AS v_b29,
    (CASE WHEN programClass like '%自然%' THEN 1 ELSE 0 END) AS v_b30,
    (CASE WHEN programClass like '%人文%' THEN 1 ELSE 0 END) AS v_b31,
    (CASE WHEN programClass like '%古装%' THEN 1 ELSE 0 END) AS v_b32,
    (CASE WHEN programClass like '%谍战%' THEN 1 ELSE 0 END) AS v_b33,
    (CASE WHEN programclass not like '%喜剧%'
    AND programclass not like '%动作%'
    AND programClass not like '%警匪%'
    AND programClass not like '%爱情%'
    AND programClass not like '%青春%'
    AND programClass not like '%犯罪%'
    AND programClass not like '%悬疑%'
    AND programClass not like '%惊悚%'
    AND programClass not like '%恐怖%'
    AND programClass not like '%灾难%'
    AND programClass not like '%战争%'
    AND programClass not like '%科幻%'
    AND programClass not like '%武侠%'
    AND programClass not like '%纪录片%'
    AND programClass not like '%体育%'
    AND programClass not like '%幼儿%'
    AND programClass not like '%动画%'
    AND programClass not like '%励志%'
    AND programClass not like '%戏曲%'
    AND programClass not like '%广场舞%'
    AND programClass not like '%真人秀%'
    AND programClass not like '%偶像%'
    AND programClass not like '%冒险%'
    AND programClass not like '%时尚%'
    AND programClass not like '%言情%'
    AND programClass not like '%足球%'
    AND programClass not like '%篮球%'
    AND programClass not like '%网球%'
    AND programClass not like '%探索%'
    AND programClass not like '%自然%'
    AND programClass not like '%人文%'
    AND programClass not like '%古装%'
    AND programClass not like '%谍战%'
    THEN 1 ELSE 0 END) AS v_b34
FROM
(
    SELECT
        deviceid,
        concat_ws("|",collect_list(programClass)) AS programClass
    FROM
    (
        SELECT
            deviceid,
            programClass
        FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer 
        WHERE date_time='20220615' AND videotype ='点播' AND playstatus='开始'
        GROUP BY deviceid,programClass
    )a 
    GROUP BY deviceid
)aa;



-----媒资标签 地区分类 标签个数：15
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_regionlabel_dm PARTITION(date_time='20220615')
SELECT 
    gg.deviceid,
    (CASE WHEN gg.region like '%内地%' THEN 1 ELSE 0 END) AS v_b1,
    (CASE WHEN gg.region like '%香港%' THEN 1 ELSE 0 END) AS v_b2,
    (CASE WHEN gg.region like '%中国台湾%' THEN 1 ELSE 0 END) AS v_b3,
    (CASE WHEN gg.region like '%日本%' THEN 1 ELSE 0 END) AS v_b4,
    (CASE WHEN gg.region like '%韩国%' THEN 1 ELSE 0 END) AS v_b5,
    (CASE WHEN gg.region like '%泰国%' THEN 1 ELSE 0 END) AS v_b6,
    (CASE WHEN gg.region like '%美国%' THEN 1 ELSE 0 END) AS v_b7,
    (CASE WHEN gg.region like '%英国%' THEN 1 ELSE 0 END) AS v_b8,
    (CASE WHEN gg.region like '%意大利%' THEN 1 ELSE 0 END) AS v_b9,
    (CASE WHEN gg.region like '%法国%' THEN 1 ELSE 0 END) AS v_b10,
    (CASE WHEN gg.region like '%俄罗斯%' THEN 1 ELSE 0 END) AS v_b11,
    (CASE WHEN gg.region like '%德国%' THEN 1 ELSE 0 END) AS v_b12,
    (CASE WHEN gg.region like '%印度%' THEN 1 ELSE 0 END) AS v_b13,
    (CASE WHEN gg.region like '%欧洲%' THEN 1 ELSE 0 END) AS v_b14,
    '' AS v_b15,
    '' AS v_b16,
    '' AS v_b17,
    '' AS v_b18,
    '' AS v_b19,
    '' AS v_b20
FROM
(
    SELECT
        ff.deviceid,
        concat_ws("|",collect_list(region)) AS region
    FROM
    (
        SELECT
            a.deviceid,
            b.region
        FROM
        (
            SELECT
                deviceid,
                programname AS videoname
            FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer 
            WHERE date_time='20220615' AND videotype ='点播' AND playstatus='开始'
            GROUP BY deviceid, programname
        )a
        LEFT JOIN
        (
            SELECT
                videoname,
                region
            FROM knowyou_ott_edw.edw_mbh_videoinformation
            WHERE videotype='点播' GROUP BY videoname, region 
        )b ON a.videoname=b.videoname
    )ff 
    GROUP BY deviceid
)gg


--------- 媒资标签 发行年限分类 标签个数：10
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_yearslabel_dm PARTITION(date_time='20220615')
SELECT 
    gg.deviceid,
    (CASE WHEN gg.datepublished like '%2022%' THEN 1 ELSE 0 END) AS v_b1,
    (CASE WHEN gg.datepublished like '%2021%' THEN 1 ELSE 0 END) AS v_b2,
    (CASE WHEN gg.datepublished like '%2020%' THEN 1 ELSE 0 END) AS v_b3,
    (CASE WHEN gg.datepublished like '%2019%' THEN 1 ELSE 0 END) AS v_b4,
    (CASE WHEN gg.datepublished like '%2018%' THEN 1 ELSE 0 END) AS v_b5,
    (CASE WHEN gg.datepublished like '%2017%' THEN 1 ELSE 0 END) AS v_b6,
    (CASE WHEN gg.datepublished like '%2010%' THEN 1 ELSE 0 END) AS v_b7,
    (CASE WHEN gg.datepublished like '%2000%' THEN 1 ELSE 0 END) AS v_b8,
    (CASE WHEN gg.datepublished like '%1990%' THEN 1 ELSE 0 END) AS v_b9,
    (CASE WHEN gg.datepublished like '%1980%' THEN 1 ELSE 0 END) AS v_b10,
    (CASE WHEN gg.datepublished like '%1970%' THEN 1 ELSE 0 END) AS v_b11,
    '' AS v_b12,
    '' AS v_b13,
    '' AS v_b14,
    '' AS v_b15,
    '' AS v_b16,
    '' AS v_b17,
    '' AS v_b18,
    '' AS v_b19,
    '' AS v_b20
FROM
(
    SELECT
        ff.deviceid,
        concat_ws("|",collect_list(datepublished)) AS datepublished
    FROM
    (
        SELECT
            a.deviceid,
            (CASE WHEN b.datepublished>= 2010 AND b.datepublished< 2016 THEN '2010'
             WHEN b.datepublished>= 2000 AND b.datepublished< 2010 THEN '2000'
             WHEN b.datepublished>= 1990 AND b.datepublished< 2000 THEN '1990'
             WHEN b.datepublished>= 1980 AND b.datepublished< 1990 THEN '1980'
             WHEN b.datepublished>= 1970 AND b.datepublished< 1980 THEN '1970'
             ELSE b.datepublished END)AS datepublished
        FROM
        (
            SELECT deviceid, programname videoname FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
            WHERE date_time='20220615' AND videotype ='点播' AND playstatus='开始'
            GROUP BY deviceid, programname
        )a
        LEFT JOIN
        (
            SELECT
                videoname,
                substr(datepublished,1,4) AS datepublished
            FROM knowyou_ott_edw.edw_mbh_videoinformation
            WHERE videotype='点播' 
            GROUP BY videoname, substr(datepublished,1,4) 
        )b ON a.videoname=b.videoname
    )ff 
    GROUP BY deviceid
)gg


--------- 媒资标签 语言分类 标签个数：9
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_languagelabel_dm PARTITION(date_time='20220615')
SELECT 
    gg.deviceid,
    (CASE WHEN gg.language like '%中文%' THEN 1 ELSE 0 END) AS v_b1,
    (CASE WHEN gg.language like '%英文%' THEN 1 ELSE 0 END) AS v_b2,
    (CASE WHEN gg.language like '%法语%' THEN 1 ELSE 0 END) AS v_b3,
    (CASE WHEN gg.language like '%德语%' THEN 1 ELSE 0 END) AS v_b4,
    (CASE WHEN gg.language like '%印度语%' THEN 1 ELSE 0 END) AS v_b5,
    (CASE WHEN gg.language like '%日语%' THEN 1 ELSE 0 END) AS v_b6,
    (CASE WHEN gg.language like '%韩语%' THEN 1 ELSE 0 END) AS v_b7,
    (CASE WHEN gg.language like '%泰语%' THEN 1 ELSE 0 END) AS v_b8,
    '' AS v_b9,
    '' AS v_b10,
    '' AS v_b11,
    '' AS v_b12,
    '' AS v_b13,
    '' AS v_b14,
    '' AS v_b15,
    '' AS v_b16,
    '' AS v_b17,
    '' AS v_b18,
    '' AS v_b19,
    '' AS v_b20
FROM
(
    SELECT
        ff.deviceid,
        concat_ws("/",collect_list(language)) AS language
    FROM
    (
        SELECT a.deviceid, b.language
        FROM
        (
            SELECT deviceid, programname videoname FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
            WHERE date_time='20220615' AND videotype ='点播' AND playstatus='开始'
            GROUP BY deviceid, programname
        )a
        LEFT JOIN
        (
            SELECT videoname, language FROM knowyou_ott_edw.edw_mbh_videoinformation
            WHERE videotype='点播' 
            GROUP BY videoname, language 
        )b ON a.videoname=b.videoname
    )ff 
    GROUP BY deviceid
)gg;


----------- 媒资标签 演员分类 标签个数：2
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_actorlabel_dm PARTITION(date_time='20220615')
SELECT 
    gg.deviceid,
    gg.actorname
FROM
(
    SELECT
        ss.deviceid,
        (CASE WHEN actorname not IN ('周迅','杨幂','赵薇','林心如','李沁','肖战','王大陆','陈道明','巩俐','陈赫','黄轩','杨采钰','成龙',
            '杨洋','李溪芮','樊少皇','于震','胡可','陈浩民','辛芷蕾','严屹宽','宁静','斯琴高娃','李倩','贾晓晨','李依晓','赵丽颖','冯绍峰',
            '倪妮','刘诗诗','唐嫣','窦骁','井柏然','钟汉良','吴京','黄晓明','杨颖','孙俪','邓超','周一围','戚薇','李现','任嘉伦','黄宗泽',
            '蒋勤勤','林更新','赵又廷','高圆圆','刘涛','邓伦','乔振宇','张卫健','童蕾','陆毅','佟大为','佟丽娅','言承旭','古天乐','张家辉',
            '刘德华','刘青云','吴彦祖','王千源','葛优','黄渤','张译','张嘉益','沈腾','徐峥','包贝尔','胡歌','彭于晏','霍建华','杨紫','乔欣',
            '蒋欣','刘嘉玲','朱一龙','吴奇隆','陈伟霆','秦海璐') THEN '其他' ELSE actorname END) AS actorname
    FROM 
    (
        SELECT
            ff.deviceid,
            concat_ws("/",collect_list(actorname)) AS actorname
        FROM
        (
            SELECT a.deviceid, b.actorname FROM
            (
                SELECT deviceid, programname videoname FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
                WHERE date_time='20220615' AND videotype ='点播' AND playstatus='开始'
                GROUP BY deviceid,programname
            )a
            LEFT JOIN
            (
                SELECT videoname, actor AS actorname FROM knowyou_ott_edw.edw_mbh_videoinformation
                WHERE videotype='点播' GROUP BY videoname, actor 
            )b ON a.videoname=b.videoname
        )ff
        GROUP BY deviceid
    )ss
    lateral view explode(split(ss.actorname,"/")) ss AS actorname
)gg
GROUP BY gg.deviceid, gg.actorname



------------ 媒资标签 导演分类 标签个数：2
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_directorlabel_dm PARTITION(date_time='20220615')
SELECT 
    gg.deviceid,
    gg.directorname
FROM
(
    SELECT
        ss.deviceid,
        (CASE WHEN directorname not IN ('尔冬升','邓超','黄渤','赵薇','周星驰','郭敬明','张艺谋','陈凯歌','高晓松','徐静蕾','徐峥',
            '王家卫','姜文','冯小刚','乌尔善','陈思诚','管虎','陈可辛','温子仁','杜琪峰','肖央','洪金宝','柳云龙','李安','姜大卫',
            '王晶','贾樟柯','曾国祥','葛民辉','北野武','黑泽明','韩三平','许鞍华','陆川','侯孝贤','郑晓龙','田壮壮','宁浩','徐克',
            '麦兆辉','娄烨','刘伟强','吴宇森','陈德森','陈木胜','刘振伟','叶伟信','陈嘉上','林超贤','李仁港','袁和平','程小东','邱礼涛',
            '唐季礼','关锦鹏','马楚成','韦家辉','高群书','王小帅','王全安','顾长卫','何平','黄建新','李玉','成龙','张扬','张一白',
            '郑保瑞','钮承泽','陈庆嘉','彭浩翔','叶念琛','尹力','叶伟民','杨树鹏','艾嘉','黄百鸣','霍建起','戴立忍','曹保') 
        THEN '其他' ELSE directorname END) AS directorname
    FROM 
    (
        SELECT
            ff.deviceid,
            concat_ws("/",collect_list(directorname)) AS directorname
        FROM
        (
            SELECT a.deviceid, b.directorname FROM
            (
                SELECT deviceid, programname videoname FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
                WHERE date_time='20220615' AND videotype ='点播' AND playstatus='开始'
                GROUP BY deviceid,programname
            )a
            LEFT JOIN
            (
                SELECT videoname, director AS directorname FROM knowyou_ott_edw.edw_mbh_videoinformation
                WHERE videotype='点播' 
                GROUP BY videoname, director 
            )b ON a.videoname=b.videoname
        )ff
        GROUP BY deviceid
    )ss
    lateral view explode(split(ss.directorname,"/")) ss AS directorname
)gg
GROUP BY gg.deviceid, gg.directorname




-- 增值标签
create table if not exists knowyou_ott_dmt.tb_mid_pdt_nettv_user_day
(
    prod_prc_id       string comment '订购产品id ',
    prod_prc_name     string comment '订购产品包名',
    prod_prc_type     string comment '订购产品类型 ',
    prod_prc_price    string comment '产品包价格 ',
    auto_continue     string comment '产品是否自动续订 ',
    begin_time        string comment '生效时间',
    end_time          string comment '失效时间',
    operation_date    string comment '订购退订时间',
    op_time           string comment '操作时间',
    order_type        string comment '订购标志',
    purchase_type     string comment '购买类型',
    purchase_id       string comment '流水',
    pay_type          string comment '支付方式',
    transaction_id    string comment '交易号',
    user_identity     string comment '用户标识'
)
comment '订购日表'
partitioned by(`deal_date_p` string comment '日期')
stored as orc tblproperties("orc.compress"="SNAPPY");

set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT overwrite TABLE knowyou_ott_dmt.tb_mid_pdt_nettv_user_day partition(deal_date_p)
select 
    product_code prod_prc_id, 
    product_name prod_prc_name, 
    '' prod_prc_type, 
    product_price prod_prc_price, 
    auto_continue, 
    begin_time, 
    end_time, 
    operation_date, 
    op_time, 
    order_type, 
    purchase_type, 
    purchase_id, 
    pay_type, 
    transaction_id, 
    t1.deviceid user_identity, 
    '20220615' deal_date_p 
from
(    
    select a.*, b.device_id from
    (
        select * from knowyou_yst_ods.ods_pd_usertv_info_d where stat_date = '20220615'
    ) a
    join 
    (
        select device_id, broadband_id from knowyou_yst_ods.ods_userbox_info_d 
        where stat_date = '20220615' and device_id is not null
        group by device_id, broadband_id
    ) b
    on a.boss_user_code = b.broadband_id
) t0
join
(
    select device_id, deviceid from
    (
        select device_id, exchange_mac from knowyou_yst_ods.ods_box_info_his_d
        where date_time = '20220615'
        group by device_id, exchange_mac
    ) a
    join
    (
        select deviceid, macaddress from knowyou_ott_edw.edw_mbh_deviceinfo
        where date_time = '20220615'
        group by deviceid, macaddress
    ) b
    on a.exchange_mac = b.macaddress
) t1
on t0.device_id = t1.device_id;




set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_demand_order_dm PARTITION(date_time='20220615')
SELECT 
    COALESCE(m1.deviceid,m2.deviceid,m3.deviceid) AS deviceid,
    (CASE WHEN m1.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS film_not_order,
    (CASE WHEN m2.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS tv_not_order,
    (CASE WHEN m3.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS child_not_order
FROM
(
    --电影收视用户且未订购
    SELECT t0.deviceid FROM
    (
        SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615' AND contentType='电影' AND playstatus ='开始' AND videotype='点播'
        GROUP BY deviceid
    )t0
    LEFT JOIN 
    (
        SELECT USER_IDENTITY FROM knowyou_ott_dmt.tb_mid_pdt_nettv_user_day
        WHERE deal_date_p='20220615' AND PROD_PRC_NAME IN ('VIP影视','电视黄金会员包','酷喵影视','VIP影视合约包','酷喵影视合约包','影视组合包','环球影视','乐享影视','精彩视界','VIP影视半年包','VIP影视（酒店）','VIP影视5折包','新星影视','欢TV','乐享影视5折包','VIP影视15元合约包（24个月）','家庭电视VIP畅享组合包','土豆影视','超人系列超人影院','超人系列超人动漫','环球影视5折包','影视组合包5折包','VIP影视年包','新星影视5折包','环球影视特价包')
        GROUP BY USER_IDENTITY
    )t1 ON t0.deviceid=t1.USER_IDENTITY
    WHERE t1.USER_IDENTITY is null
)m1
FULL JOIN
(
    --电视剧收视用户且未订购
    SELECT t0.deviceid FROM
    (
        SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615' AND contentType='电视剧' AND playstatus ='开始' AND videotype='点播'
        GROUP BY deviceid
    )t0
    LEFT JOIN 
    (
        SELECT USER_IDENTITY FROM knowyou_ott_dmt.tb_mid_pdt_nettv_user_day
        WHERE deal_date_p='20220615' AND PROD_PRC_NAME IN ('VIP影视','电视黄金会员包','酷喵影视','VIP影视合约包','酷喵影视合约包','影视组合包','环球影视','乐享影视','精彩视界','VIP影视半年包','VIP影视（酒店）','VIP影视5折包','新星影视','欢TV','乐享影视5折包','VIP影视15元合约包（24个月）','家庭电视VIP畅享组合包','土豆影视','超人系列超人影院','超人系列超人动漫','环球影视5折包','影视组合包5折包','VIP影视年包','新星影视5折包','环球影视特价包')
        GROUP BY USER_IDENTITY
    )t1 ON t0.deviceid=t1.USER_IDENTITY
    WHERE t1.USER_IDENTITY is null
)m2 ON m1.deviceid=m2.deviceid
FULL JOIN
(
    --少儿收视用户且未订购
    SELECT t0.deviceid FROM
    (
        SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615' AND contentType='少儿' AND playstatus ='开始' AND videotype='点播'
        GROUP BY deviceid
    )t0
    LEFT JOIN 
    (
        SELECT USER_IDENTITY FROM knowyou_ott_dmt.tb_mid_pdt_nettv_user_day
        WHERE deal_date_p='20220615' AND PROD_PRC_NAME IN ('VIP少儿','影视少儿频道包','VIP少儿合约包','奇趣星球','看视少儿','拉贝少儿','狐趣星球','炫力动漫','智趣尊享包','爱看视界少儿学堂','快乐城堡开心乐园','嘟嘟玩具王国','家有萌宝','儿童乐园','少儿组合包','宝宝乐园','迪士尼','嗨皮城堡','快乐宝贝','超级萌童','淘淘玩具屋','泡泡玩乐屋','VIP少儿半年包','叮当动漫','拉贝少儿5折包','VIP少儿5折包','知了乐园','优宝乐园','童趣漫游','童学优漫','玩具乐园','乐学星空','快乐佳贝','童趣漫游5折包','儿童乐园5折包','童画绘本','少儿天地','童学优漫5折包','看视少儿5折包','少儿组合包5折包','迪士尼专区5折包','VIP少儿年包','幺儿乐知堂','优优乐园','小小少儿','奇异乐园','橙子乐园','启蒙快乐包')
        GROUP BY USER_IDENTITY
    )t1 ON t0.deviceid=t1.USER_IDENTITY
    WHERE t1.USER_IDENTITY is null
)m3 ON m1.deviceid=m3.deviceid



-- 电影点播类别偏好未订购表
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_film_order_dm PARTITION(date_time)
SELECT 
    p1.deviceid,
    (CASE WHEN p2.USER_IDENTITY IS NULL AND p1.film_prefer_light=1 THEN 1 ELSE 0 END) AS film_light_not_order,
    (CASE WHEN p2.USER_IDENTITY IS NULL AND p1.film_prefer_middle=1 THEN 1 ELSE 0 END) AS film_middle_not_order,
    (CASE WHEN p2.USER_IDENTITY IS NULL AND p1.film_prefer_high=1 THEN 1 ELSE 0 END) AS film_high_not_order,
    '20220615' AS date_time
FROM
(
    SELECT 
        COALESCE(m1.deviceid,m2.deviceid,m3.deviceid) AS deviceid,
        (CASE WHEN m1.deviceid is not null THEN 1 ELSE 0 END) AS film_prefer_light,
        (CASE WHEN m2.deviceid is not null THEN 1 ELSE 0 END) AS film_prefer_middle,
        (CASE WHEN m3.deviceid is not null THEN 1 ELSE 0 END) AS film_prefer_high
    FROM
    (
        SELECT t0.deviceid FROM
        (
            SELECT deviceid, sum(playtime) AS playduration FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
            WHERE date_time='20220615' AND contentType='电影' AND playstatus ='开始' AND videotype='点播' 
            GROUP BY deviceid
        )t0 
        WHERE t0.playduration<3600
    )m1
    FULL JOIN
    (
        SELECT t1.deviceid FROM
        (
            SELECT deviceid, sum(playtime) AS playduration FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
            WHERE date_time='20220615' AND contentType='电影' AND playstatus ='开始' AND videotype='点播' 
            GROUP BY deviceid
        )t1 
        WHERE t1.playduration>=3600 AND t1.playduration<=10800
    )m2 ON m1.deviceid=m2.deviceid
    FULL JOIN
    (
        SELECT t2.deviceid FROM
        (
            SELECT deviceid, sum(playtime) AS playduration FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
            WHERE date_time='20220615' AND contentType='电影' AND playstatus ='开始' AND videotype='点播' 
            GROUP BY deviceid
        )t2 
        WHERE t2.playduration>10800
    )m3 ON m1.deviceid=m3.deviceid
)p1
LEFT JOIN 
(
    SELECT USER_IDENTITY FROM knowyou_ott_dmt.tb_mid_pdt_nettv_user_day
    WHERE deal_date_p='20220615' AND PROD_PRC_NAME IN ('VIP影视','电视黄金会员包','酷喵影视','VIP影视合约包','酷喵影视合约包','影视组合包','环球影视','乐享影视','精彩视界','VIP影视半年包','VIP影视（酒店）','VIP影视5折包','新星影视','欢TV','乐享影视5折包','VIP影视15元合约包（24个月）','家庭电视VIP畅享组合包','土豆影视','超人系列超人影院','超人系列超人动漫','环球影视5折包','影视组合包5折包','VIP影视年包','新星影视5折包','环球影视特价包')
    GROUP BY USER_IDENTITY
)p2 ON p1.deviceid=p2.USER_IDENTITY;



-- 电视剧点播类别偏好未订购表
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_tv_order_dm PARTITION(date_time)
SELECT 
    p1.deviceid,
    (CASE WHEN p2.USER_IDENTITY IS NULL AND p1.tv_prefer_light=1 THEN 1 ELSE 0 END) AS tv_light_not_order,
    (CASE WHEN p2.USER_IDENTITY IS NULL AND p1.tv_prefer_middle=1 THEN 1 ELSE 0 END) AS tv_middle_not_order,
    (CASE WHEN p2.USER_IDENTITY IS NULL AND p1.tv_prefer_high=1 THEN 1 ELSE 0 END) AS tv_high_not_order,
    '20220615' AS date_time
FROM
(
    SELECT 
        COALESCE(m1.deviceid,m2.deviceid,m3.deviceid) AS deviceid,
        (CASE WHEN m1.deviceid is not null THEN 1 ELSE 0 END) AS tv_prefer_light,
        (CASE WHEN m2.deviceid is not null THEN 1 ELSE 0 END) AS tv_prefer_middle,
        (CASE WHEN m3.deviceid is not null THEN 1 ELSE 0 END) AS tv_prefer_high
    FROM
    (
        SELECT t0.deviceid FROM
        (
            SELECT deviceid, sum(playtime) AS playduration FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
            WHERE date_time='20220615' AND contentType='电视剧' AND playstatus ='开始' AND videotype='点播' 
            GROUP BY deviceid
        )t0 
        WHERE t0.playduration<3600
    )m1
    FULL JOIN
    (
        SELECT t1.deviceid FROM
        (
            SELECT deviceid,sum(playtime) AS playduration FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
            WHERE date_time='20220615' AND contentType='电视剧' AND playstatus ='开始' AND videotype='点播' 
            GROUP BY deviceid
        )t1 
        WHERE t1.playduration>=3600 AND t1.playduration<=10800
    )m2 ON m1.deviceid=m2.deviceid
    FULL JOIN
    (
        SELECT t2.deviceid FROM
        (
            SELECT deviceid,sum(playtime) AS playduration FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
            WHERE date_time='20220615' AND contentType='电视剧' AND playstatus ='开始' AND videotype='点播' 
            GROUP BY deviceid
        )t2 
        WHERE t2.playduration>10800
    )m3 ON m1.deviceid=m3.deviceid
)p1
LEFT JOIN 
(
    SELECT USER_IDENTITY FROM knowyou_ott_dmt.tb_mid_pdt_nettv_user_day
    WHERE deal_date_p='20220615' AND PROD_PRC_NAME IN ('VIP影视','电视黄金会员包','酷喵影视','VIP影视合约包','酷喵影视合约包','影视组合包','环球影视','乐享影视','精彩视界','VIP影视半年包','VIP影视（酒店）','VIP影视5折包','新星影视','欢TV','乐享影视5折包','VIP影视15元合约包（24个月）','家庭电视VIP畅享组合包','土豆影视','超人系列超人影院','超人系列超人动漫','环球影视5折包','影视组合包5折包','VIP影视年包','新星影视5折包','环球影视特价包')
    GROUP BY USER_IDENTITY
)p2 ON p1.deviceid=p2.USER_IDENTITY;


-- 少儿点播类别偏好未订购表
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_child_order_dm PARTITION(date_time)
SELECT 
    p1.deviceid,
    (CASE WHEN p2.USER_IDENTITY IS NULL AND p1.child_prefer_light=1 THEN 1 ELSE 0 END) AS child_light_not_order,
    (CASE WHEN p2.USER_IDENTITY IS NULL AND p1.child_prefer_middle=1 THEN 1 ELSE 0 END) AS child_middle_not_order,
    (CASE WHEN p2.USER_IDENTITY IS NULL AND p1.child_prefer_high=1 THEN 1 ELSE 0 END) AS child_high_not_order,
    '20220615' AS date_time
FROM
(
    SELECT 
        COALESCE(m1.deviceid,m2.deviceid,m3.deviceid) AS deviceid,
        (CASE WHEN m1.deviceid is not null THEN 1 ELSE 0 END) AS child_prefer_light,
        (CASE WHEN m2.deviceid is not null THEN 1 ELSE 0 END) AS child_prefer_middle,
        (CASE WHEN m3.deviceid is not null THEN 1 ELSE 0 END) AS child_prefer_high
    FROM
    (
        SELECT t0.deviceid FROM
        (
            SELECT deviceid,sum(playtime) AS playduration FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
            WHERE date_time='20220615' AND contentType='少儿' AND playstatus ='开始' AND videotype='点播' 
            GROUP BY deviceid
        )t0 
        WHERE t0.playduration<3600
    )m1
    FULL JOIN
    (
        SELECT t1.deviceid FROM
        (
            SELECT deviceid,sum(playtime) AS playduration FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
            WHERE date_time='20220615' AND contentType='少儿' AND playstatus ='开始' AND videotype='点播' 
            GROUP BY deviceid
        )t1 
        WHERE t1.playduration>=3600 AND t1.playduration<=10800
    )m2 ON m1.deviceid=m2.deviceid
    FULL JOIN
    (
        SELECT t2.deviceid FROM
        (
            SELECT deviceid,sum(playtime) AS playduration FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
            WHERE date_time='20220615' AND contentType='少儿' AND playstatus ='开始' AND videotype='点播' 
            GROUP BY deviceid
        )t2 
        WHERE t2.playduration>10800
    )m3 ON m1.deviceid=m3.deviceid
)p1
LEFT JOIN (
    SELECT USER_IDENTITY FROM knowyou_ott_dmt.tb_mid_pdt_nettv_user_day
    WHERE deal_date_p='20220615' AND PROD_PRC_NAME IN ('VIP少儿','影视少儿频道包','VIP少儿合约包','奇趣星球','看视少儿','拉贝少儿','狐趣星球','炫力动漫','智趣尊享包','爱看视界少儿学堂','快乐城堡开心乐园','嘟嘟玩具王国','家有萌宝','儿童乐园','少儿组合包','宝宝乐园','迪士尼','嗨皮城堡','快乐宝贝','超级萌童','淘淘玩具屋','泡泡玩乐屋','VIP少儿半年包','叮当动漫','拉贝少儿5折包','VIP少儿5折包','知了乐园','优宝乐园','童趣漫游','童学优漫','玩具乐园','乐学星空','快乐佳贝','童趣漫游5折包','儿童乐园5折包','童画绘本','少儿天地','童学优漫5折包','看视少儿5折包','少儿组合包5折包','迪士尼专区5折包','VIP少儿年包','幺儿乐知堂','优优乐园','小小少儿','奇异乐园','橙子乐园','启蒙快乐包')
    GROUP BY USER_IDENTITY
)p2 ON p1.deviceid=p2.USER_IDENTITY




--------------------------------------------------------------------------------------------
--存疑  每日付费存量订购
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_stock_order PARTITION(date_time='20220615')
SELECT

FROM
(
    -- 影视存量订购
    SELECT 
        deviceid, 
        CASE WHEN USER_IDENTITY IS NOT NULL THEN 1 ELSE 0 END AS tv_type_stock_order
    FROM
    (
        SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615' AND (contentType='电视剧' OR contentType='电影') AND playstatus='开始' AND videotype='点播'
        GROUP BY deviceid
    ) t0
    LEFT JOIN 
    (
        SELECT USER_IDENTITY FROM knowyou_ott_dmt.tb_mid_pdt_nettv_user_day
        WHERE deal_date_p='20220615' AND PROD_PRC_NAME IN ()
        GROUP BY USER_IDENTITY
    ) t1 ON t0.deviceid = t1.USER_IDENTITY
) a
FULL JOIN
(
    -- 少儿存量订购
    SELECT 
        deviceid, 
        CASE WHEN USER_IDENTITY IS NOT NULL THEN 1 ELSE 0 END AS children_type_stock_order
    FROM
    (
        SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615' AND contentType='少儿' AND playstatus='开始' AND videotype='点播'
        GROUP BY deviceid
    ) t0
    LEFT JOIN 
    (
        SELECT USER_IDENTITY FROM knowyou_ott_dmt.tb_mid_pdt_nettv_user_day
        WHERE deal_date_p='20220615' AND PROD_PRC_NAME IN ('魔百和-少儿包包月10元','IPTV-少儿包包月15元','IPTV少儿30天18元','IPTV少儿包季39.9元','IPTV少儿包半年78.8元','IPTV少儿包年')
        GROUP BY USER_IDENTITY
    ) t1 ON t0.deviceid = t1.USER_IDENTITY
) c ON a.deviceid = c.deviceid
FULL JOIN
(
    -- 教育存量订购
    SELECT 
        deviceid, 
        CASE WHEN USER_IDENTITY IS NOT NULL THEN 1 ELSE 0 END AS education_type_stock_order
    FROM
    (
        SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615' AND contentType='教育' AND playstatus='开始' AND videotype='点播'
        GROUP BY deviceid
    ) t0
    LEFT JOIN 
    (
        SELECT USER_IDENTITY FROM knowyou_ott_dmt.tb_mid_pdt_nettv_user_day
        WHERE deal_date_p='20220615' AND PROD_PRC_NAME IN ()
        GROUP BY USER_IDENTITY
    ) t1 ON t0.deviceid = t1.USER_IDENTITY
) d ON a.deviceid = d.deviceid
FULL JOIN
(
    -- 电竞存量订购
    SELECT 
        deviceid, 
        CASE WHEN USER_IDENTITY IS NOT NULL THEN 1 ELSE 0 END AS gaming_type_stock_order
    FROM
    (
        SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615' AND contentType='电竞' AND playstatus='开始' AND videotype='点播'
        GROUP BY deviceid
    ) t0
    LEFT JOIN 
    (
        SELECT USER_IDENTITY FROM knowyou_ott_dmt.tb_mid_pdt_nettv_user_day
        WHERE deal_date_p='20220615' AND PROD_PRC_NAME IN ()
        GROUP BY USER_IDENTITY
    ) t1 ON t0.deviceid = t1.USER_IDENTITY
) e ON a.deviceid = e.deviceid
FULL JOIN
(
    -- 体育存量订购
    SELECT 
        deviceid, 
        CASE WHEN USER_IDENTITY IS NOT NULL THEN 1 ELSE 0 END AS sport_type_stock_order
    FROM
    (
        SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615' AND contentType='体育' AND playstatus='开始' AND videotype='点播'
        GROUP BY deviceid
    ) t0
    LEFT JOIN 
    (
        SELECT USER_IDENTITY FROM knowyou_ott_dmt.tb_mid_pdt_nettv_user_day
        WHERE deal_date_p='20220615' AND PROD_PRC_NAME IN ()
        GROUP BY USER_IDENTITY
    ) t1 ON t0.deviceid = t1.USER_IDENTITY
) f ON a.deviceid = f.deviceid
FULL JOIN
(
    -- 游戏存量订购
    SELECT 
        deviceid, 
        CASE WHEN USER_IDENTITY IS NOT NULL THEN 1 ELSE 0 END AS game_type_stock_order
    FROM
    (
        SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615' AND contentType='游戏' AND playstatus='开始' AND videotype='点播'
        GROUP BY deviceid
    ) t0
    LEFT JOIN 
    (
        SELECT USER_IDENTITY FROM knowyou_ott_dmt.tb_mid_pdt_nettv_user_day
        WHERE deal_date_p='20220615' AND PROD_PRC_NAME IN ()
        GROUP BY USER_IDENTITY
    ) t1 ON t0.deviceid = t1.USER_IDENTITY
) g ON a.deviceid = g.deviceid
FULL JOIN
(
    -- 其他存量订购
    SELECT 
        deviceid, 
        CASE WHEN USER_IDENTITY IS NOT NULL THEN 1 ELSE 0 END AS other_type_stock_order
    FROM
    (
        SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615' AND playstatus='开始' AND videotype='点播' AND contentType!='电视剧' AND contentType!='电影' 
        AND contentType!='少儿' AND contentType!='教育' AND contentType!='电竞' AND contentType!='体育' AND contentType!='游戏'
        GROUP BY deviceid
    ) t0
    LEFT JOIN 
    (
        SELECT USER_IDENTITY FROM knowyou_ott_dmt.tb_mid_pdt_nettv_user_day
        WHERE deal_date_p='20220615' AND PROD_PRC_NAME IN ()
        GROUP BY USER_IDENTITY
    ) t1 ON t0.deviceid = t1.USER_IDENTITY
) h ON a.deviceid = h.deviceid
FULL JOIN
(
    -- 极光TVVIP存量订购
    SELECT 
        deviceid, 
        CASE WHEN USER_IDENTITY IS NOT NULL THEN 1 ELSE 0 END AS jiguang_tvvip_stock_order
    FROM
    (
        SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615' AND appname='tv.huan.tencentTV'
        GROUP BY deviceid
    ) t0
    LEFT JOIN 
    (
        SELECT USER_IDENTITY FROM knowyou_ott_dmt.tb_mid_pdt_nettv_user_day
        WHERE deal_date_p='20220615' AND PROD_PRC_NAME IN ()
        GROUP BY USER_IDENTITY
    ) t1 ON t0.deviceid = t1.USER_IDENTITY
) i ON a.deviceid = i.deviceid
FULL JOIN
(
    -- 芒果TVVIP存量订购
    SELECT 
        deviceid, 
        CASE WHEN USER_IDENTITY IS NOT NULL THEN 1 ELSE 0 END AS mangguo_tvvip_stock_order
    FROM
    (
        SELECT deviceid FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
        WHERE date_time='20220615' AND appname='tv.huan.tencentTV'
        GROUP BY deviceid
    ) t0
    LEFT JOIN 
    (
        SELECT USER_IDENTITY FROM knowyou_ott_dmt.tb_mid_pdt_nettv_user_day
        WHERE deal_date_p='20220615' AND PROD_PRC_NAME IN ()
        GROUP BY USER_IDENTITY
    ) t1 ON t0.deviceid = t1.USER_IDENTITY
) i ON a.deviceid = i.deviceid

--------------------------------------------------------------------------------------------




-- 用户画像 分时段标签
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_content_prefer_dm PARTITION(date_time)
SELECT	
deviceid,
contenttype,
hourtype,
ranks,
'20220615' AS date_time
-- 分区时间
FROM(
SELECT b.deviceid,b.contenttype,b.hourtype,row_number() over(PARTITION by b.deviceid,b.hourtype ORDER BY b.play_num desc) AS ranks
FROM(
SELECT a.deviceid,a.contenttype,a.hourtype,count(*) AS play_num
FROM(
SELECT deviceid,contenttype,
(CASE WHEN substr(actiontime,9,2) >='00' AND substr(actiontime,9,2)<'06'  THEN 1
WHEN substr(actiontime,9,2) >='06' AND substr(actiontime,9,2)<'11' THEN 2
WHEN substr(actiontime,9,2) >='11' AND substr(actiontime,9,2)<'16' THEN 3
WHEN substr(actiontime,9,2) >='16' AND substr(actiontime,9,2)<='23' THEN 4
ELSE 1 END)  AS hourtype -- 小时段分类 
FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer 
WHERE date_time>'20220614' AND date_time <='20220615'  --近一个月
AND contenttype IN ('少儿','综艺','电影','电视剧','动漫','电竞','生活','纪实','音乐','体育','教育','爱家教育','新闻','云游戏','游戏') -- 可以设置类别固定内容类型
union
SELECT deviceid,contenttype,
(CASE WHEN substr(actiontime,9,2) >='00' AND substr(actiontime,9,2)<'06'  THEN 1
WHEN substr(actiontime,9,2) >='06' AND substr(actiontime,9,2)<'11' THEN 2
WHEN substr(actiontime,9,2) >='11' AND substr(actiontime,9,2)<'16' THEN 3
WHEN substr(actiontime,9,2) >='16' AND substr(actiontime,9,2)<='23' THEN 4
ELSE 1 END)  AS hourtype -- 小时段分类 
FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer 
WHERE date_time>'20220614' AND date_time <='20220615'  --近一个月
AND contenttype IN ('少儿','综艺','电影','电视剧','动漫','电竞','生活','纪实','音乐','体育','教育','爱家教育','新闻','云游戏','游戏') -- 可以设置类别固定内容类型
)a 
GROUP BY a.deviceid,a.contenttype,a.hourtype
)b
)c WHERE c.ranks<=3




-- 2.用户画像 分时段epg栏目top偏好
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_epg_prefer_dm PARTITION(date_time)
SELECT
deviceid,
t1.fivelevel AS fivelevel,
hourtype,
ranks,
'20220615' AS date_time --分区时间
FROM
(SELECT	
deviceid,
fivelevel,
hourtype,
ranks
FROM(
SELECT b.deviceid,b.fivelevel,b.hourtype,row_number() over(PARTITION by b.deviceid,b.hourtype ORDER BY b.play_num desc) AS ranks
FROM(
SELECT a.deviceid,a.fivelevel,a.hourtype,count(*) AS play_num
FROM(
SELECT deviceid,contenttype AS fivelevel,
(CASE WHEN substr(actiontime,9,2) >='00' AND substr(actiontime,9,2)<'06'  THEN 1
WHEN substr(actiontime,9,2) >='06' AND substr(actiontime,9,2)<'11' THEN 2
WHEN substr(actiontime,9,2) >='11' AND substr(actiontime,9,2)<'16' THEN 3
WHEN substr(actiontime,9,2) >='16' AND substr(actiontime,9,2)<='23' THEN 4
ELSE 1 END)  AS hourtype -- 小时段分类 
FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer
WHERE date_time>'20220614' AND date_time <'20220602'  --近一个月
AND contenttype !='$' AND contenttype!=''
UNION ALL 
SELECT deviceid,listsubtype AS fivelevel,
(CASE WHEN substr(actiontime,9,2) >='00' AND substr(actiontime,9,2)<'06'  THEN 1
WHEN substr(actiontime,9,2) >='06' AND substr(actiontime,9,2)<'11' THEN 2
WHEN substr(actiontime,9,2) >='11' AND substr(actiontime,9,2)<'16' THEN 3
WHEN substr(actiontime,9,2) >='16' AND substr(actiontime,9,2)<='23' THEN 4
ELSE 1 END)  AS hourtype 
FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer 
WHERE date_time>'20220614' AND date_time <'20220602' AND listsubtype!='' AND listsubtype IS NOT NULL 
)a 
GROUP BY a.deviceid,a.fivelevel,a.hourtype
)b
)c WHERE c.ranks<=5
)t1


SELECT luacpversion,collect_set(listsubtype) FROM knowyou_ott_ods.ods_mbh_userbehavior_buffer WHERE date_time>=20220615 GROUP BY luacpversion




-- 3. 用户特征
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_user_feature_dm PARTITION(date_time)

SELECT coalesce(t1.deviceid,t2.deviceid,'') AS deviceid,
t1.top1_content,t1.top2_content,t1.top3_content,t2.top1_epg,
t2.top2_epg,t2.top3_epg,t2.top4_epg,t2.top5_epg,
'1' AS hourtype,
coalesce(t1.date_time,t2.date_time,'') AS date_time
FROM(
SELECT	a.deviceid,
coalesce(a.contenttype,'') AS top1_content,
coalesce(b.contenttype,'') AS top2_content,
coalesce(c.contenttype,'') AS top3_content,
a.date_time
FROM(
SELECT deviceid,contenttype,date_time FROM knowyou_ott_dmt.htv_content_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='1' AND ranks='1'
)a
LEFT JOIN(
SELECT deviceid,contenttype,date_time FROM knowyou_ott_dmt.htv_content_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='1' AND ranks='2'
)b ON a.deviceid=b.deviceid
LEFT JOIN(
SELECT deviceid,contenttype,date_time FROM knowyou_ott_dmt.htv_content_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='1' AND ranks='3'
)c ON a.deviceid=c.deviceid
)t1
FULL JOIN(
SELECT	a.deviceid,
coalesce(a.fivelevel,'') AS top1_epg,
coalesce(b.fivelevel,'') AS top2_epg,
coalesce(c.fivelevel,'') AS top3_epg,
coalesce(d.fivelevel,'') AS top4_epg,
coalesce(e.fivelevel,'') AS top5_epg,
a.date_time
FROM(
SELECT deviceid,fivelevel,date_time FROM knowyou_ott_dmt.htv_epg_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='1' AND ranks='1'
)a
LEFT JOIN(
SELECT deviceid,fivelevel,date_time FROM knowyou_ott_dmt.htv_epg_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='1' AND ranks='2'
)b ON a.deviceid=b.deviceid
LEFT JOIN(
SELECT deviceid,fivelevel,date_time FROM knowyou_ott_dmt.htv_epg_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='1' AND ranks='3'
)c ON a.deviceid=c.deviceid
LEFT JOIN(
SELECT deviceid,fivelevel,date_time FROM knowyou_ott_dmt.htv_epg_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='1' AND ranks='4'
)d ON a.deviceid=d.deviceid
LEFT JOIN(
SELECT deviceid,fivelevel,date_time FROM knowyou_ott_dmt.htv_epg_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='1' AND ranks='5'
)e ON a.deviceid=e.deviceid
)t2 
ON t1.deviceid=t2.deviceid

UNION ALL

SELECT coalesce(t1.deviceid,t2.deviceid,'') AS deviceid,
t1.top1_content,t1.top2_content,t1.top3_content,t2.top1_epg,
t2.top2_epg,t2.top3_epg,t2.top4_epg,t2.top5_epg,
'2' AS hourtype,
coalesce(t1.date_time,t2.date_time,'') AS date_time
FROM(
SELECT	a.deviceid,
coalesce(a.contenttype,'') AS top1_content,
coalesce(b.contenttype,'') AS top2_content,
coalesce(c.contenttype,'') AS top3_content,
a.date_time
FROM(
SELECT deviceid,contenttype,date_time FROM knowyou_ott_dmt.htv_content_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='2' AND ranks='1'
)a
LEFT JOIN(
SELECT deviceid,contenttype,date_time FROM knowyou_ott_dmt.htv_content_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='2' AND ranks='2'
)b ON a.deviceid=b.deviceid
LEFT JOIN(
SELECT deviceid,contenttype,date_time FROM knowyou_ott_dmt.htv_content_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='2' AND ranks='3'
)c ON a.deviceid=c.deviceid
)t1
FULL JOIN(
SELECT	a.deviceid,
coalesce(a.fivelevel,'') AS top1_epg,
coalesce(b.fivelevel,'') AS top2_epg,
coalesce(c.fivelevel,'') AS top3_epg,
coalesce(d.fivelevel,'') AS top4_epg,
coalesce(e.fivelevel,'') AS top5_epg,
a.date_time
FROM(
SELECT deviceid,fivelevel,date_time FROM knowyou_ott_dmt.htv_epg_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='2' AND ranks='1'
)a
LEFT JOIN(
SELECT deviceid,fivelevel,date_time FROM knowyou_ott_dmt.htv_epg_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='2' AND ranks='2'
)b ON a.deviceid=b.deviceid
LEFT JOIN(
SELECT deviceid,fivelevel,date_time FROM knowyou_ott_dmt.htv_epg_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='2' AND ranks='3'
)c ON a.deviceid=c.deviceid
LEFT JOIN(
SELECT deviceid,fivelevel,date_time FROM knowyou_ott_dmt.htv_epg_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='2' AND ranks='4'
)d ON a.deviceid=d.deviceid
LEFT JOIN(
SELECT deviceid,fivelevel,date_time FROM knowyou_ott_dmt.htv_epg_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='2' AND ranks='5'
)e ON a.deviceid=e.deviceid
)t2 
ON t1.deviceid=t2.deviceid

UNION ALL 

SELECT coalesce(t1.deviceid,t2.deviceid,'') AS deviceid,
t1.top1_content,t1.top2_content,t1.top3_content,t2.top1_epg,
t2.top2_epg,t2.top3_epg,t2.top4_epg,t2.top5_epg,
'3' AS hourtype,
coalesce(t1.date_time,t2.date_time,'') AS date_time
FROM(
SELECT	a.deviceid,
coalesce(a.contenttype,'') AS top1_content,
coalesce(b.contenttype,'') AS top2_content,
coalesce(c.contenttype,'') AS top3_content,
a.date_time
FROM(
SELECT deviceid,contenttype,date_time FROM knowyou_ott_dmt.htv_content_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='3' AND ranks='1'
)a
LEFT JOIN(
SELECT deviceid,contenttype,date_time FROM knowyou_ott_dmt.htv_content_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='3' AND ranks='2'
)b ON a.deviceid=b.deviceid
LEFT JOIN(
SELECT deviceid,contenttype,date_time FROM knowyou_ott_dmt.htv_content_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='3' AND ranks='3'
)c ON a.deviceid=c.deviceid
)t1
FULL JOIN(
SELECT	a.deviceid,
coalesce(a.fivelevel,'') AS top1_epg,
coalesce(b.fivelevel,'') AS top2_epg,
coalesce(c.fivelevel,'') AS top3_epg,
coalesce(d.fivelevel,'') AS top4_epg,
coalesce(e.fivelevel,'') AS top5_epg,
a.date_time
FROM(
SELECT deviceid,fivelevel,date_time FROM knowyou_ott_dmt.htv_epg_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='3' AND ranks='1'
)a
LEFT JOIN(
SELECT deviceid,fivelevel,date_time FROM knowyou_ott_dmt.htv_epg_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='3' AND ranks='2'
)b ON a.deviceid=b.deviceid
LEFT JOIN(
SELECT deviceid,fivelevel,date_time FROM knowyou_ott_dmt.htv_epg_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='3' AND ranks='3'
)c ON a.deviceid=c.deviceid
LEFT JOIN(
SELECT deviceid,fivelevel,date_time FROM knowyou_ott_dmt.htv_epg_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='3' AND ranks='4'
)d ON a.deviceid=d.deviceid
LEFT JOIN(
SELECT deviceid,fivelevel,date_time FROM knowyou_ott_dmt.htv_epg_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='3' AND ranks='5'
)e ON a.deviceid=e.deviceid
)t2 
ON t1.deviceid=t2.deviceid

UNION ALL 

SELECT coalesce(t1.deviceid,t2.deviceid,'') AS deviceid,
t1.top1_content,t1.top2_content,t1.top3_content,t2.top1_epg,
t2.top2_epg,t2.top3_epg,t2.top4_epg,t2.top5_epg,
'4' AS hourtype,
coalesce(t1.date_time,t2.date_time,'') AS date_time
FROM(
SELECT	a.deviceid,
coalesce(a.contenttype,'') AS top1_content,
coalesce(b.contenttype,'') AS top2_content,
coalesce(c.contenttype,'') AS top3_content,
a.date_time
FROM(
SELECT deviceid,contenttype,date_time FROM knowyou_ott_dmt.htv_content_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='4' AND ranks='1'
)a
LEFT JOIN(
SELECT deviceid,contenttype,date_time FROM knowyou_ott_dmt.htv_content_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='4' AND ranks='2'
)b ON a.deviceid=b.deviceid
LEFT JOIN(
SELECT deviceid,contenttype,date_time FROM knowyou_ott_dmt.htv_content_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='4' AND ranks='3'
)c ON a.deviceid=c.deviceid
)t1
FULL JOIN(
SELECT	a.deviceid,
coalesce(a.fivelevel,'') AS top1_epg,
coalesce(b.fivelevel,'') AS top2_epg,
coalesce(c.fivelevel,'') AS top3_epg,
coalesce(d.fivelevel,'') AS top4_epg,
coalesce(e.fivelevel,'') AS top5_epg,
a.date_time
FROM(
SELECT deviceid,fivelevel,date_time FROM knowyou_ott_dmt.htv_epg_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='4' AND ranks='1'
)a
LEFT JOIN(
SELECT deviceid,fivelevel,date_time FROM knowyou_ott_dmt.htv_epg_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='4' AND ranks='2'
)b ON a.deviceid=b.deviceid
LEFT JOIN(
SELECT deviceid,fivelevel,date_time FROM knowyou_ott_dmt.htv_epg_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='4' AND ranks='3'
)c ON a.deviceid=c.deviceid
LEFT JOIN(
SELECT deviceid,fivelevel,date_time FROM knowyou_ott_dmt.htv_epg_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='4' AND ranks='4'
)d ON a.deviceid=d.deviceid
LEFT JOIN(
SELECT deviceid,fivelevel,date_time FROM knowyou_ott_dmt.htv_epg_prefer_dm 
WHERE date_time = '20220615'  AND hourtype='4' AND ranks='5'
)e ON a.deviceid=e.deviceid
)t2 
ON t1.deviceid=t2.deviceid 




-- 4.用户分时段评分明细表
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_user_rating_detail_dm PARTITION(date_time)

SELECT  a.deviceid,
a.hourtype,
coalesce(m1.child_rate ,'0') AS child_top1_content_rate,
coalesce(m1.teen_rate ,'0') AS teen_top1_content_rate,
coalesce(m1.youth_rate ,'0') AS youth_top1_content_rate,
coalesce(m1.mid_rate ,'0') AS mid_top1_content_rate,
coalesce(m1.old_rate ,'0') AS old_top1_content_rate,
coalesce(m1.male_rate ,'0') AS male_top1_content_rate,
coalesce(m1.female_rate ,'0') AS female_top1_content_rate,

coalesce(0.8*m2.child_rate ,'0') AS child_top2_content_rate,
coalesce(0.8*m2.teen_rate ,'0') AS teen_top2_content_rate,
coalesce(0.8*m2.youth_rate ,'0') AS youth_top2_content_rate,
coalesce(0.8*m2.mid_rate ,'0') AS mid_top2_content_rate,
coalesce(0.8*m2.old_rate ,'0') AS old_top2_content_rate,
coalesce(0.8*m2.male_rate ,'0') AS male_top2_content_rate,
coalesce(0.8*m2.female_rate ,'0') AS female_top2_content_rate,

coalesce(0.6*m3.child_rate ,'0') AS child_top3_content_rate,
coalesce(0.6*m3.teen_rate ,'0') AS teen_top3_content_rate,
coalesce(0.6*m3.youth_rate ,'0') AS youth_top3_content_rate,
coalesce(0.6*m3.mid_rate ,'0') AS mid_top3_content_rate,
coalesce(0.6*m3.old_rate ,'0') AS old_top3_content_rate,
coalesce(0.6*m3.male_rate ,'0') AS male_top3_content_rate,
coalesce(0.6*m3.female_rate ,'0') AS female_top3_content_rate,

coalesce(t1.child_rate ,'0') AS child_top1_epg_rate,
coalesce(t1.teen_rate ,'0') AS teen_top1_epg_rate,
coalesce(t1.youth_rate ,'0') AS youth_top1_epg_rate,
coalesce(t1.mid_rate ,'0') AS mid_top1_epg_rate,
coalesce(t1.old_rate ,'0') AS old_top1_epg_rate,
coalesce(t1.male_rate ,'0') AS male_top1_epg_rate,
coalesce(t1.female_rate ,'0') AS female_top1_epg_rate,

coalesce(0.8*t2.child_rate ,'0') AS child_top2_epg_rate,
coalesce(0.8*t2.teen_rate ,'0') AS teen_top2_epg_rate,
coalesce(0.8*t2.youth_rate ,'0') AS youth_top2_epg_rate,
coalesce(0.8*t2.mid_rate ,'0') AS mid_top2_epg_rate,
coalesce(0.8*t2.old_rate ,'0') AS old_top2_epg_rate,
coalesce(0.8*t2.male_rate ,'0') AS male_top2_epg_rate,
coalesce(0.8*t2.female_rate ,'0') AS female_top2_epg_rate,

coalesce(0.6*t3.child_rate ,'0') AS child_top3_epg_rate,
coalesce(0.6*t3.teen_rate ,'0') AS teen_top3_epg_rate,
coalesce(0.6*t3.youth_rate ,'0') AS youth_top3_epg_rate,
coalesce(0.6*t3.mid_rate ,'0') AS mid_top3_epg_rate,
coalesce(0.6*t3.old_rate ,'0') AS old_top3_epg_rate,
coalesce(0.6*t3.male_rate ,'0') AS male_top3_epg_rate,
coalesce(0.6*t3.female_rate ,'0') AS female_top3_epg_rate,

coalesce(0.4*t4.child_rate ,'0') AS child_top4_epg_rate,
coalesce(0.4*t4.teen_rate ,'0') AS teen_top4_epg_rate,
coalesce(0.4*t4.youth_rate ,'0') AS youth_top4_epg_rate,
coalesce(0.4*t4.mid_rate ,'0') AS mid_top4_epg_rate,
coalesce(0.4*t4.old_rate ,'0') AS old_top4_epg_rate,
coalesce(0.4*t4.male_rate ,'0') AS male_top4_epg_rate,
coalesce(0.4*t4.female_rate ,'0') AS female_top4_epg_rate,

coalesce(0.2*t5.child_rate ,'0') AS child_top5_epg_rate,
coalesce(0.2*t5.teen_rate ,'0') AS teen_top5_epg_rate,
coalesce(0.2*t5.youth_rate ,'0') AS youth_top5_epg_rate,
coalesce(0.2*t5.mid_rate ,'0') AS mid_top5_epg_rate,
coalesce(0.2*t5.old_rate ,'0') AS old_top5_epg_rate,
coalesce(0.2*t5.male_rate ,'0') AS male_top5_epg_rate,
coalesce(0.2*t5.female_rate ,'0') AS female_top5_epg_rate,
'20220615' AS date_time
FROM(
SELECT * FROM knowyou_ott_dmt.htv_user_feature_dm
WHERE date_time = '20220615' 
)a
LEFT JOIN (
SELECT * FROM knowyou_ott_dmt.imp_contenttype_grade
)m1 
ON a.top1_content=m1.content_type
LEFT JOIN (
SELECT * FROM knowyou_ott_dmt.imp_contenttype_grade
)m2 
ON a.top2_content=m2.content_type
LEFT JOIN (
SELECT * FROM knowyou_ott_dmt.imp_contenttype_grade
)m3 
ON a.top3_content=m3.content_type
LEFT JOIN (
SELECT * FROM knowyou_ott_dmt.imp_epg_type_grade
)t1 
ON a.top1_epg=t1.epg
LEFT JOIN (
SELECT * FROM knowyou_ott_dmt.imp_epg_type_grade
)t2 
ON a.top2_epg=t2.epg
LEFT JOIN (
SELECT * FROM knowyou_ott_dmt.imp_epg_type_grade
)t3 
ON a.top3_epg=t3.epg
LEFT JOIN (
SELECT * FROM knowyou_ott_dmt.imp_epg_type_grade
)t4 
ON a.top4_epg=t4.epg
LEFT JOIN (
SELECT * FROM knowyou_ott_dmt.imp_epg_type_grade
)t5 
ON a.top5_epg=t5.epg 








-- 5.用户分时段总评分表
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_user_rating_dm PARTITION(date_time)

SELECT  deviceid,
hourtype,
(child_top1_content_rate + child_top2_content_rate + child_top3_content_rate +child_top1_epg_rate 
+child_top2_epg_rate +child_top3_epg_rate +child_top4_epg_rate+child_top5_epg_rate) AS rate,	
'child' AS grouptype,
date_time
FROM knowyou_ott_dmt.htv_user_rating_detail_dm WHERE date_time='20220615'
UNION ALL 
SELECT  deviceid,
hourtype,
(teen_top1_content_rate + teen_top2_content_rate + teen_top3_content_rate +teen_top1_epg_rate 
+teen_top2_epg_rate +teen_top3_epg_rate +teen_top4_epg_rate+teen_top5_epg_rate) AS rate,
'teen' AS grouptype,
date_time
FROM knowyou_ott_dmt.htv_user_rating_detail_dm WHERE date_time='20220615'
UNION ALL
SELECT  deviceid,
hourtype,
(youth_top1_content_rate + youth_top2_content_rate + youth_top3_content_rate +youth_top1_epg_rate 
+youth_top2_epg_rate +youth_top3_epg_rate +youth_top4_epg_rate+youth_top5_epg_rate) AS rate,
'youth' AS grouptype,
date_time
FROM knowyou_ott_dmt.htv_user_rating_detail_dm WHERE date_time='20220615'
UNION ALL
SELECT  deviceid,
hourtype,
(mid_top1_content_rate + mid_top2_content_rate + mid_top3_content_rate +mid_top1_epg_rate 
+mid_top2_epg_rate +mid_top3_epg_rate +mid_top4_epg_rate+mid_top5_epg_rate) AS rate,
'mid' AS grouptype,
date_time
FROM knowyou_ott_dmt.htv_user_rating_detail_dm WHERE date_time='20220615'
UNION ALL
SELECT  deviceid,
hourtype,
(old_top1_content_rate + old_top2_content_rate + old_top3_content_rate +old_top1_epg_rate 
+old_top2_epg_rate +old_top3_epg_rate +old_top4_epg_rate+old_top5_epg_rate) AS rate,
'old' AS grouptype,
date_time
FROM knowyou_ott_dmt.htv_user_rating_detail_dm WHERE date_time='20220615'
UNION ALL
SELECT  deviceid,
hourtype,
(male_top1_content_rate + male_top2_content_rate + male_top3_content_rate +male_top1_epg_rate 
+male_top2_epg_rate +male_top3_epg_rate +male_top4_epg_rate+male_top5_epg_rate) AS rate,
'male' AS grouptype,
date_time
FROM knowyou_ott_dmt.htv_user_rating_detail_dm WHERE date_time='20220615'
UNION ALL
SELECT  deviceid,
hourtype,
(female_top1_content_rate + female_top2_content_rate + female_top3_content_rate +female_top1_epg_rate 
+female_top2_epg_rate +female_top3_epg_rate +female_top4_epg_rate+female_top5_epg_rate) AS rate,	
'female' AS grouptype,
date_time
FROM knowyou_ott_dmt.htv_user_rating_detail_dm WHERE date_time='20220615' 





--用户画像
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_user_personas_dm PARTITION(date_time)
SELECT  m1.deviceid,
m1.hourtype,
m1.grouptype AS grouptype ,
m2.grouptype AS sex_type,
'20220615' AS date_time
FROM(
SELECT t1.deviceid,t1.hourtype,t1.grouptype
FROM(
SELECT  deviceid,
hourtype,
grouptype,
row_number() over(PARTITION by deviceid,hourtype ORDER BY rate desc) AS ranks
FROM knowyou_ott_dmt.htv_user_rating_dm
WHERE date_time='20220615' AND grouptype IN ('child','teen','youth','mid','old')
)t1 
WHERE t1.ranks=1
)m1
LEFT JOIN (
SELECT t2.deviceid,t2.hourtype,t2.grouptype
FROM(
SELECT  deviceid,
hourtype,
grouptype,
row_number() over(PARTITION by deviceid,hourtype ORDER BY rate desc) AS ranks 
FROM knowyou_ott_dmt.htv_user_rating_dm 
WHERE date_time='20220615' AND grouptype IN ('male','female')
)t2 
WHERE t2.ranks=1
)m2 ON m1.deviceid=m2.deviceid AND m1.hourtype=m2.hourtype 







