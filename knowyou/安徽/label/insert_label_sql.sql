--        ┏┓　　　┏┓+ +
--　　　┏┛┻━━━┛┻┓ + +
--　　　┃　　　　　　　┃ 　
--　　　┃　　　━　　　┃ ++ + + +
--　　 ████━████ ┃+
--　　　┃　　　　　　　┃ +
--　　　┃　　　┻　　　┃
--　　　┃　　　　　　　┃ + +
--　　　┗━┓　　　┏━┛
--　　　　　┃　　　┃　　　　　　　　　　　
--　　　　　┃　　　┃ + + + +
--　　　　　┃　　　┃　　　　Codes are far away from bugs with the animal protecting　　　
--　　　　　┃　　　┃ + 　　　　神兽保佑,代码无bug　　
--　　　　　┃　　　┃
--　　　　　┃　　　┃　　+　　　　　　　　　
--　　　　　┃　 　　┗━━━┓ + +
--　　　　　┃ 　　　　　　　┣┓
--　　　　　┃ 　　　　　　　┏┛
--　　　　　┗┓┓┏━┳┓┏┛ + + + +
--　　　　　　┃┫┫　┃┫┫
--　　　　　　┗┻┛　┗┻┛+ + + +

-- 1.1 行为标签-用户活跃信息-基础信息标签表（当日/当月）
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_serv_basic_part1 PARTITION(dt='${C_DAY}')
SELECT 
    a.deviceid AS device_id,
    (CASE WHEN bootstatusday IS NOT NULL THEN bootstatusday ELSE 0 END) AS is_boot, -- 当日是否开机
    (CASE WHEN playstatusday IS NOT NULL THEN playstatusday ELSE 0 END) AS is_play, -- 当日是否播放
    (CASE WHEN boot_num IS NOT NULL THEN boot_num ELSE 0 END) AS boot_num, -- 当日开机次数
    (CASE WHEN c.play_duration IS NOT NULL THEN c.play_duration ELSE 0 END) AS play_duration, -- 当日播放时长
    (CASE WHEN c.play_num IS NOT NULL THEN c.play_num ELSE 0 END) AS play_num , -- 当日播放次数
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS is_live, -- 当日收看直播
    (CASE WHEN d.play_num IS NOT NULL THEN d.play_num ELSE 0 END) AS live_play_num, -- 当日收看直播次数
    (CASE WHEN d.play_duration IS NOT NULL THEN d.play_duration ELSE 0 END) AS live_play_duration, -- 当日收看直播时长
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS is_demAND, -- 当日收看点播
    (CASE WHEN e.play_num IS NOT NULL THEN e.play_num ELSE 0 END) AS demAND_play_num, --当日收看点播次数
    (CASE WHEN e.play_duration IS NOT NULL THEN e.play_duration ELSE 0 END) AS demAND_play_duration, -- 当日收看点播时长
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS is_replay, -- 当日收看回看
    (CASE WHEN f.play_num IS NOT NULL THEN f.play_num ELSE 0 END) AS replay_play_num, --当日收看回看次数
    (CASE WHEN f.play_duration IS NOT NULL THEN f.play_duration ELSE 0 END) AS replay_play_duration -- 当日收看回看时长 
FROM
(
    SELECT deviceid 
    FROM knowyou_ott_ods.dws_all_video_playinfo_di 
    WHERE dt='${C_DAY}'
    GROUP BY deviceid
) a
LEFT JOIN 
(
    -- 当日是否开机/是否播放 
    SELECT deviceid,  MAX(bootstatusday) bootstatusday, MAX(playstatusday) playstatusday
    FROM knowyou_ott_ods.dim_mbh_user_itv_df 
    WHERE dt='${C_DAY}' 
    GROUP BY deviceid
) b ON a.deviceid = b.deviceid
LEFT JOIN
(
    -- 当日开机次数
    SELECT deviceid, count(*) boot_num
    FROM knowyou_ott_ods.odm_jituan_info_tmp
    WHERE dt='${C_DAY}' AND code = '13'
    GROUP BY deviceid
) s ON a.deviceid = s.deviceid
LEFT JOIN
( 
    -- 当日播放时长\当日播放次数
    SELECT deviceid, count(*) AS play_num, round(sum(playTime)/3600, 1) AS play_duration
    FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt='${C_DAY}' 
    GROUP BY deviceid
) c ON a.deviceid = c.deviceid
LEFT JOIN
(
    -- 当日收看直播次数\时长
    SELECT deviceid, count(*) AS play_num, round(sum(playTime)/3600, 1) AS play_duration
    FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt='${C_DAY}' AND videotype = '直播'
    GROUP BY deviceid
) d ON a.deviceid = d.deviceid 
LEFT JOIN
(
    -- 当日收看点播次数\时长
    SELECT deviceid, count(*) AS play_num, round(sum(playTime)/3600, 1) AS play_duration
    FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt='${C_DAY}' AND videotype = '点播'
    GROUP BY deviceid
) e ON a.deviceid = e.deviceid
LEFT JOIN
(
    -- 当日收看回播次数\时长
    SELECT deviceid, count(*) AS play_num,round(sum(playTime)/3600, 1) AS play_duration
    FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt='${C_DAY}' AND videotype = '回看'
    GROUP BY deviceid
) f ON a.deviceid = f.deviceid
;


INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_serv_basic_part2 PARTITION(dt='${C_DAY}')
SELECT a.deviceid AS device_id,
    (CASE WHEN bootstatusday IS NOT NULL THEN bootstatusday ELSE 0 END) AS month_is_boot, 
    (CASE WHEN playstatusday IS NOT NULL THEN playstatusday ELSE 0 END) AS month_is_play, 
    (CASE WHEN boot_num IS NOT NULL THEN boot_num ELSE 0 END) AS month_boot_num, 
    (CASE WHEN c.play_duration IS NOT NULL THEN c.play_duration ELSE 0 END) AS month_play_duration, 
    (CASE WHEN c.play_num IS NOT NULL THEN c.play_num ELSE 0 END) AS month_play_num, 
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_is_live, 
    (CASE WHEN d.play_num IS NOT NULL THEN d.play_num ELSE 0 END) AS month_live_play_num, 
    (CASE WHEN d.play_duration IS NOT NULL THEN d.play_duration ELSE 0 END) AS month_live_play_duration, 
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_is_demAND, 
    (CASE WHEN e.play_num IS NOT NULL THEN e.play_num ELSE 0 END) AS month_demAND_play_num, 
    (CASE WHEN e.play_duration IS NOT NULL THEN e.play_duration ELSE 0 END) AS month_demAND_play_duration, 
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_is_replay, 
    (CASE WHEN f.play_num IS NOT NULL THEN f.play_num ELSE 0 END) AS month_replay_play_num, 
    (CASE WHEN f.play_duration IS NOT NULL THEN f.play_duration ELSE 0 END) AS month_replay_play_duration 
FROM
(
    SELECT deviceid 
    FROM knowyou_ott_ods.dws_all_video_playinfo_di 
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01'
    GROUP BY deviceid
) a
LEFT JOIN 
(
    -- 当月是否开机/是否播放 
    SELECT deviceid, MAX(bootstatusday) bootstatusday, MAX(playstatusday) playstatusday
    FROM knowyou_ott_ods.dim_mbh_user_itv_df 
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' 
    GROUP BY deviceid
) b ON a.deviceid = b.deviceid
LEFT JOIN
(
    -- 当月开机次数
    SELECT deviceid, count(*) boot_num
    FROM knowyou_ott_ods.odm_jituan_info_tmp
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND code = '13'
    GROUP BY deviceid
) s ON a.deviceid = s.deviceid
LEFT JOIN
( 
    -- 当月播放时长\当日播放次数
    SELECT deviceid, count(*) AS play_num, round(sum(playTime)/3600, 1) AS play_duration
    FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' 
    GROUP BY deviceid
) c ON a.deviceid = c.deviceid
LEFT JOIN
(
    -- 当月收看直播次数\时长
    SELECT deviceid, count(*) AS play_num, round(sum(playTime)/3600, 1) AS play_duration
    FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND videotype = '直播'
    GROUP BY deviceid
) d ON a.deviceid = d.deviceid 
LEFT JOIN
(
    -- 当月收看点播次数\时长
    SELECT deviceid, count(*) AS play_num, round(sum(playTime)/3600, 1) AS play_duration
    FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND videotype = '点播'
    GROUP BY deviceid
) e ON a.deviceid = e.deviceid
LEFT JOIN
(
    -- 当月收看回播次数\时长
    SELECT deviceid, count(*) AS play_num,round(sum(playTime)/3600, 1) AS play_duration
    FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND videotype = '回看'
    GROUP BY deviceid
) f ON a.deviceid = f.deviceid
;

INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_serv_basic_total PARTITION(dt = '${C_DAY}')
SELECT
    device_id,                    user_sub_id,                NVL(is_boot,0),              NVL(is_play,0), 
    NVL(boot_num,0),              NVL(play_duration,0),       NVL(play_num,0),             NVL(is_live,0), 
    NVL(live_play_num,0),         NVL(live_play_duration,0),  NVL(is_demAND,0),            NVL(demAND_play_num,0), 
    NVL(demAND_play_duration,0),  NVL(is_replay,0),           NVL(replay_play_num,0),      NVL(replay_play_duration,0), 
    month_is_boot,         month_is_play,          month_boot_num,       month_play_duration, 
    month_play_num,        month_is_live,          month_live_play_num,  month_live_play_duration, 
    month_is_demAND,       month_demAND_play_num,  month_demAND_play_duration, 
    month_is_replay,       month_replay_play_num,  month_replay_play_duration
FROM
(
    SELECT
        NVL(a.device_id, b.device_id) device_id,       is_boot,              is_play, 
        boot_num,              play_duration,          play_num,             is_live, 
        live_play_num,         live_play_duration,     is_demAND,            demAND_play_num, 
        demAND_play_duration,  is_replay,              replay_play_num,      replay_play_duration, 
        month_is_boot,         month_is_play,          month_boot_num,       month_play_duration, 
        month_play_num,        month_is_live,          month_live_play_num,  month_live_play_duration, 
        month_is_demAND,       month_demAND_play_num,  month_demAND_play_duration, 
        month_is_replay,       month_replay_play_num,  month_replay_play_duration
    FROM
    (
        SELECT * FROM knowyou_ott_dmt.htv_serv_basic_part1 
        WHERE dt = '${C_DAY}'
    ) a
    FULL JOIN
    (
        SELECT * FROM knowyou_ott_dmt.htv_serv_basic_part2 
        WHERE dt = '${C_DAY}'
    ) b ON a.device_id = b.device_id 
) t1
LEFT JOIN
(
    SELECT user_sub_id, mac FROM
    (
        SELECT user_sub_id, mac, row_number() over(partition by mac order by active_time desc) rn
        FROM knowyou_ott_ods.dwd_jf_user_itv_df
        WHERE dt = '${C_DAY}' AND length(mac) = 17
    ) t WHERE rn = 1
) t2 ON lower(substr(t1.device_id, -12, 12)) = lower(REGEXP_REPLACE(t2.mac, ':', ''))
;

--
--                       .::::.
--                     .::::::::.
--                    :::::::::::
--                 ..:::::::::::'
--              '::::::::::::'
--                .::::::::::
--           '::::::::::::::..
--                ..::::::::::::.
--              ``::::::::::::::::
--               ::::``:::::::::'        .:::.
--              ::::'   ':::::'       .::::::::.
--            .::::'      ::::     .:::::::'::::.
--           .:::'       :::::  .:::::::::' ':::::.
--          .::'        :::::.:::::::::'      ':::::.
--         .::'         ::::::::::::::'         ``::::.
--     ...:::           ::::::::::::'              ``::.
--    ```` ':.          ':::::::::'                  ::::..
--                       '.:::::'                    ':'````..
-- 1.2 行为标签-用户活跃信息-点播、直播、回看各时段标签表（当日/当月）
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_demAND_prefer_part1 PARTITION(dt = '${C_DAY}')
SELECT 
    a.deviceid device_id,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demAND_0_is_watched,
    (CASE WHEN c.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demAND_6_is_watched,
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demAND_9_is_watched,
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demAND_12_is_watched,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demAND_14_is_watched,
    (CASE WHEN g.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demAND_18_is_watched,
    (CASE WHEN h.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS demAND_22_is_watched
FROM
(
    SELECT deviceid 
    FROM knowyou_ott_ods.dws_all_video_playinfo_di 
    WHERE dt='${C_DAY}'
    GROUP BY deviceid
)a
LEFT JOIN
(
    -- 当日点播时段0-6点
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt = '${C_DAY}' AND videotype = '点播'
    AND substr(actualtime,9,2) >= '00' AND substr(actualtime,9,2) < '06'
    GROUP BY deviceid
)b ON a.deviceid = b.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt = '${C_DAY}' AND videotype = '点播'
    AND substr(actualtime,9,2) >= '06' AND substr(actualtime,9,2) < '09'
    GROUP BY deviceid
)c ON a.deviceid = c.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt = '${C_DAY}' AND videotype = '点播'
    AND substr(actualtime,9,2) >= '09' AND substr(actualtime,9,2) < '12'
    GROUP BY deviceid
)d ON a.deviceid = d.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt = '${C_DAY}' AND videotype = '点播'
    AND substr(actualtime,9,2) >= '12' AND substr(actualtime,9,2) < '14'
    GROUP BY deviceid
)e ON a.deviceid = e.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt = '${C_DAY}' AND videotype = '点播'
    AND substr(actualtime,9,2) >= '14' AND substr(actualtime,9,2) < '18'
    GROUP BY deviceid
)f ON a.deviceid = f.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt = '${C_DAY}' AND videotype = '点播'
    AND substr(actualtime,9,2) >= '18' AND substr(actualtime,9,2) < '22'
    GROUP BY deviceid
)g ON a.deviceid = g.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt = '${C_DAY}' AND videotype = '点播'
    AND substr(actualtime,9,2) >= '22' AND substr(actualtime,9,2) < '24'
    GROUP BY deviceid
)h ON a.deviceid = h.deviceid
;

INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_live_prefer_part1 PARTITION(dt = '${C_DAY}')
SELECT 
    a.deviceid AS device_id,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS live_0_is_watched,
    (CASE WHEN c.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS live_6_is_watched,
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS live_9_is_watched,
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS live_12_is_watched,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS live_14_is_watched,
    (CASE WHEN g.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS live_18_is_watched,
    (CASE WHEN h.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS live_22_is_watched
FROM
(
    SELECT deviceid 
    FROM knowyou_ott_ods.dws_all_video_playinfo_di 
    WHERE dt='${C_DAY}'
    GROUP BY deviceid
) a
-- 当日直播时段0-6点偏好
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt='${C_DAY}' AND videotype = '直播'
    AND substr(actualtime,9,2) >= '00' AND substr(actualtime,9,2) < '06'
    GROUP BY deviceid
) b ON a.deviceid=b.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt='${C_DAY}' AND videotype = '直播'
    AND substr(actualtime,9,2) >= '06' AND substr(actualtime,9,2) < '09'
    GROUP BY deviceid
) c ON a.deviceid=c.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt='${C_DAY}' AND videotype = '直播'
    AND substr(actualtime,9,2) >= '09' AND substr(actualtime,9,2) < '12'
    GROUP BY deviceid
) d ON a.deviceid=d.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt='${C_DAY}' AND videotype = '直播'
    AND substr(actualtime,9,2) >= '12' AND substr(actualtime,9,2) < '14'
    GROUP BY deviceid
) e ON a.deviceid=e.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt='${C_DAY}' AND videotype = '直播'
    AND substr(actualtime,9,2) >= '14' AND substr(actualtime,9,2) < '18'
    GROUP BY deviceid
) f ON a.deviceid=f.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt='${C_DAY}' AND videotype = '直播'
    AND substr(actualtime,9,2) >= '18' AND substr(actualtime,9,2) < '22'
    GROUP BY deviceid
) g ON a.deviceid=g.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt='${C_DAY}' AND videotype = '直播'
    AND substr(actualtime,9,2) >= '22' AND substr(actualtime,9,2) < '24'
    GROUP BY deviceid
) h ON a.deviceid=h.deviceid
;

INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_replay_prefer_part1 PARTITION(dt = '${C_DAY}')
SELECT 
    a.deviceid AS device_id,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS replay_0_is_watched,
    (CASE WHEN c.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS replay_6_is_watched,
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS replay_9_is_watched,
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS replay_12_is_watched,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS replay_14_is_watched,
    (CASE WHEN g.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS replay_18_is_watched,
    (CASE WHEN h.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS replay_22_is_watched
FROM
(
    SELECT deviceid 
    FROM knowyou_ott_ods.dws_all_video_playinfo_di 
    WHERE dt='${C_DAY}'
    GROUP BY deviceid
) a
-- 当日回看时段0-6点偏好
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt='${C_DAY}' AND videotype = '回看'
    AND substr(actualtime,9,2) >= '00' AND substr(actualtime,9,2) < '06'
    GROUP BY deviceid
) b ON a.deviceid=b.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt='${C_DAY}' AND videotype = '回看'
    AND substr(actualtime,9,2) >= '06' AND substr(actualtime,9,2) < '09'
    GROUP BY deviceid
) c ON a.deviceid=c.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt='${C_DAY}' AND videotype = '回看'
    AND substr(actualtime,9,2) >= '09' AND substr(actualtime,9,2) < '12'
    GROUP BY deviceid
) d ON a.deviceid=d.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt='${C_DAY}' AND videotype = '回看'
    AND substr(actualtime,9,2) >= '12' AND substr(actualtime,9,2) < '14'
    GROUP BY deviceid
) e ON a.deviceid=e.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt='${C_DAY}' AND videotype = '回看'
    AND substr(actualtime,9,2) >= '14' AND substr(actualtime,9,2) < '18'
    GROUP BY deviceid
) f ON a.deviceid=f.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt='${C_DAY}' AND videotype = '回看'
    AND substr(actualtime,9,2) >= '18' AND substr(actualtime,9,2) < '22'
    GROUP BY deviceid
) g ON a.deviceid=g.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt='${C_DAY}' AND videotype = '回看'
    AND substr(actualtime,9,2) >= '22' AND substr(actualtime,9,2) < '24'
    GROUP BY deviceid
) h ON a.deviceid=h.deviceid
;

INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_demAND_prefer_part2 PARTITION(dt = '${C_DAY}')
SELECT 
    a.deviceid device_id,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_demAND_0_is_watched,
    (CASE WHEN c.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_demAND_6_is_watched,
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_demAND_9_is_watched,
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_demAND_12_is_watched,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_demAND_14_is_watched,
    (CASE WHEN g.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_demAND_18_is_watched,
    (CASE WHEN h.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_demAND_22_is_watched
FROM
(
    SELECT deviceid 
    FROM knowyou_ott_ods.dws_all_video_playinfo_di 
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' 
    GROUP BY deviceid
)a
LEFT JOIN
(
    -- 当日点播时段0-6点
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01'  AND videotype = '点播'
    AND substr(actualtime,9,2) >= '00' AND substr(actualtime,9,2) < '06'
    GROUP BY deviceid
)b ON a.deviceid = b.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01'  AND videotype = '点播'
    AND substr(actualtime,9,2) >= '06' AND substr(actualtime,9,2) < '09'
    GROUP BY deviceid
)c ON a.deviceid = c.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01'  AND videotype = '点播'
    AND substr(actualtime,9,2) >= '09' AND substr(actualtime,9,2) < '12'
    GROUP BY deviceid
)d ON a.deviceid = d.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01'  AND videotype = '点播'
    AND substr(actualtime,9,2) >= '12' AND substr(actualtime,9,2) < '14'
    GROUP BY deviceid
)e ON a.deviceid = e.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01'  AND videotype = '点播'
    AND substr(actualtime,9,2) >= '14' AND substr(actualtime,9,2) < '18'
    GROUP BY deviceid
)f ON a.deviceid = f.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01'  AND videotype = '点播'
    AND substr(actualtime,9,2) >= '18' AND substr(actualtime,9,2) < '22'
    GROUP BY deviceid
)g ON a.deviceid = g.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01'  AND videotype = '点播'
    AND substr(actualtime,9,2) >= '22' AND substr(actualtime,9,2) < '24'
    GROUP BY deviceid
)h ON a.deviceid = h.deviceid
;

INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_live_prefer_part2 PARTITION(dt = '${C_DAY}')
SELECT 
    a.deviceid AS device_id,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_live_0_is_watched,
    (CASE WHEN c.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_live_6_is_watched,
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_live_9_is_watched,
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_live_12_is_watched,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_live_14_is_watched,
    (CASE WHEN g.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_live_18_is_watched,
    (CASE WHEN h.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_live_22_is_watched
FROM
(
    SELECT deviceid 
    FROM knowyou_ott_ods.dws_all_video_playinfo_di 
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' 
    GROUP BY deviceid
) a
-- 当日直播时段0-6点偏好
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND videotype = '直播'
    AND substr(actualtime,9,2) >= '00' AND substr(actualtime,9,2) < '06'
    GROUP BY deviceid
) b ON a.deviceid=b.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND videotype = '直播'
    AND substr(actualtime,9,2) >= '06' AND substr(actualtime,9,2) < '09'
    GROUP BY deviceid
) c ON a.deviceid=c.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND videotype = '直播'
    AND substr(actualtime,9,2) >= '09' AND substr(actualtime,9,2) < '12'
    GROUP BY deviceid
) d ON a.deviceid=d.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND videotype = '直播'
    AND substr(actualtime,9,2) >= '12' AND substr(actualtime,9,2) < '14'
    GROUP BY deviceid
) e ON a.deviceid=e.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND videotype = '直播'
    AND substr(actualtime,9,2) >= '14' AND substr(actualtime,9,2) < '18'
    GROUP BY deviceid
) f ON a.deviceid=f.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND videotype = '直播'
    AND substr(actualtime,9,2) >= '18' AND substr(actualtime,9,2) < '22'
    GROUP BY deviceid
) g ON a.deviceid=g.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND videotype = '直播'
    AND substr(actualtime,9,2) >= '22' AND substr(actualtime,9,2) < '24'
    GROUP BY deviceid
) h ON a.deviceid=h.deviceid
;

INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_replay_prefer_part2 PARTITION(dt = '${C_DAY}')
SELECT 
    a.deviceid AS device_id,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_replay_0_is_watched,
    (CASE WHEN c.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_replay_6_is_watched,
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_replay_9_is_watched,
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_replay_12_is_watched,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_replay_14_is_watched,
    (CASE WHEN g.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_replay_18_is_watched,
    (CASE WHEN h.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_replay_22_is_watched
FROM
(
    SELECT deviceid 
    FROM knowyou_ott_ods.dws_all_video_playinfo_di 
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' 
    GROUP BY deviceid
) a
-- 当日回看时段0-6点偏好
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND videotype = '回看'
    AND substr(actualtime,9,2) >= '00' AND substr(actualtime,9,2) < '06'
    GROUP BY deviceid
) b ON a.deviceid=b.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND videotype = '回看'
    AND substr(actualtime,9,2) >= '06' AND substr(actualtime,9,2) < '09'
    GROUP BY deviceid
) c ON a.deviceid=c.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND videotype = '回看'
    AND substr(actualtime,9,2) >= '09' AND substr(actualtime,9,2) < '12'
    GROUP BY deviceid
) d ON a.deviceid=d.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND videotype = '回看'
    AND substr(actualtime,9,2) >= '12' AND substr(actualtime,9,2) < '14'
    GROUP BY deviceid
) e ON a.deviceid=e.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND videotype = '回看'
    AND substr(actualtime,9,2) >= '14' AND substr(actualtime,9,2) < '18'
    GROUP BY deviceid
) f ON a.deviceid=f.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND videotype = '回看'
    AND substr(actualtime,9,2) >= '18' AND substr(actualtime,9,2) < '22'
    GROUP BY deviceid
) g ON a.deviceid=g.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND videotype = '回看'
    AND substr(actualtime,9,2) >= '22' AND substr(actualtime,9,2) < '24'
    GROUP BY deviceid
) h ON a.deviceid=h.deviceid
;


INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_demAND_live_replay_total PARTITION(dt = '${C_DAY}')
SELECT
    device_id,      user_sub_id,
    NVL(demAND_0_is_watched, 0), NVL(demAND_6_is_watched, 0),  NVL(demAND_9_is_watched, 0), NVL(demAND_12_is_watched, 0),
    NVL(demAND_14_is_watched, 0),NVL(demAND_18_is_watched, 0), NVL(demAND_22_is_watched, 0),
    month_demAND_0_is_watched,   month_demAND_6_is_watched,    month_demAND_9_is_watched,   month_demAND_12_is_watched,
    month_demAND_14_is_watched,  month_demAND_18_is_watched,   month_demAND_22_is_watched,

    NVL(live_0_is_watched, 0),   NVL(live_6_is_watched, 0),    NVL(live_9_is_watched, 0),   NVL(live_12_is_watched, 0),
    NVL(live_14_is_watched, 0),  NVL(live_18_is_watched, 0),   NVL(live_22_is_watched, 0),
    month_live_0_is_watched,     month_live_6_is_watched,      month_live_9_is_watched,     month_live_12_is_watched,
    month_live_14_is_watched,    month_live_18_is_watched,     month_live_22_is_watched,

    NVL(replay_0_is_watched, 0), NVL(replay_6_is_watched, 0),  NVL(replay_9_is_watched, 0), NVL(replay_12_is_watched, 0),
    NVL(replay_14_is_watched, 0),NVL(replay_18_is_watched, 0), NVL(replay_22_is_watched, 0),
    month_replay_0_is_watched,   month_replay_6_is_watched,    month_replay_9_is_watched,   month_replay_12_is_watched,
    month_replay_14_is_watched,  month_replay_18_is_watched,   month_replay_22_is_watched              
FROM
(
    SELECT
        COALESCE(a.device_id,b.device_id,c.device_id,d.device_id,e.device_id,f.device_id) device_id,       
        demAND_0_is_watched,   demAND_6_is_watched,    demAND_9_is_watched,   demAND_12_is_watched,
        demAND_14_is_watched,  demAND_18_is_watched,   demAND_22_is_watched,
        live_0_is_watched,     live_6_is_watched,      live_9_is_watched,     live_12_is_watched,
        live_14_is_watched,    live_18_is_watched,     live_22_is_watched,
        replay_0_is_watched,   replay_6_is_watched,    replay_9_is_watched,   replay_12_is_watched,
        replay_14_is_watched,  replay_18_is_watched,   replay_22_is_watched,
        month_demAND_0_is_watched,   month_demAND_6_is_watched,    month_demAND_9_is_watched,   month_demAND_12_is_watched,
        month_demAND_14_is_watched,  month_demAND_18_is_watched,   month_demAND_22_is_watched,
        month_live_0_is_watched,     month_live_6_is_watched,      month_live_9_is_watched,     month_live_12_is_watched,
        month_live_14_is_watched,    month_live_18_is_watched,     month_live_22_is_watched,
        month_replay_0_is_watched,   month_replay_6_is_watched,    month_replay_9_is_watched,   month_replay_12_is_watched,
        month_replay_14_is_watched,  month_replay_18_is_watched,   month_replay_22_is_watched
    FROM
    (
        SELECT * FROM knowyou_ott_dmt.htv_demAND_prefer_part2 
        WHERE dt = '${C_DAY}'
    ) a
    FULL JOIN
    (
        SELECT * FROM knowyou_ott_dmt.htv_live_prefer_part2
        WHERE dt = '${C_DAY}'
    ) b ON a.device_id = b.device_id 
    FULL JOIN
    (
        SELECT * FROM knowyou_ott_dmt.htv_replay_prefer_part2
        WHERE dt = '${C_DAY}'
    ) c ON a.device_id = c.device_id 
    FULL JOIN
    (
        SELECT * FROM knowyou_ott_dmt.htv_demAND_prefer_part1 
        WHERE dt = '${C_DAY}'
    ) d ON a.device_id = d.device_id 
    FULL JOIN
    (
        SELECT * FROM knowyou_ott_dmt.htv_live_prefer_part1
        WHERE dt = '${C_DAY}'
    ) e ON a.device_id = e.device_id 
    FULL JOIN
    (
        SELECT * FROM knowyou_ott_dmt.htv_replay_prefer_part1
        WHERE dt = '${C_DAY}'
    ) f ON a.device_id = f.device_id 
) t1
LEFT JOIN
(
    SELECT user_sub_id, mac FROM
    (
        SELECT user_sub_id, mac, row_number() over(partition by mac order by active_time desc) rn
        FROM knowyou_ott_ods.dwd_jf_user_itv_df
        WHERE dt = '${C_DAY}' AND length(mac) = 17
    ) t WHERE rn = 1
) t2 ON lower(substr(t1.device_id, -12, 12)) = lower(REGEXP_REPLACE(t2.mac, ':', ''))
;

-- *
--                                         ,s555SB@@&                          
--                                      :9H####@@@@@Xi                        
--                                     1@@@@@@@@@@@@@@8                       
--                                   ,8@@@@@@@@@B@@@@@@8                      
--                                  :B@@@@X3hi8Bs;B@@@@@Ah,                   
--             ,8i                  r@@@B:     1S ,M@@@@@@#8;                 
--            1AB35.i:               X@@8 .   SGhr ,A@@@@@@@@S                
--            1@h31MX8                18Hhh3i .i3r ,A@@@@@@@@@5               
--            ;@&i,58r5                 rGSS:     :B@@@@@@@@@@A               
--             1#i  . 9i                 hX.  .: .5@@@@@@@@@@@1               
--              sG1,  ,G53s.              9#Xi;hS5 3B@@@@@@@B1                
--               .h8h.,A@@@MXSs,           #@H1:    3ssSSX@1                  
--               s ,@@@@@@@@@@@@Xhi,       r#@@X1s9M8    .GA981               
--               ,. rS8H#@@@@@@@@@@#HG51;.  .h31i;9@r    .8@@@@BS;i;          
--                .19AXXXAB@@@@@@@@@@@@@@#MHXG893hrX#XGGXM@@@@@@@@@@MS        
--                s@@MM@@@hsX#@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@&,      
--              :GB@#3G@@Brs ,1GM@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@B,     
--            .hM@@@#@@#MX 51  r;iSGAM@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@8     
--          :3B@@@@@@@@@@@&9@h :Gs   .;sSXH@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@:    
--      s&HA#@@@@@@@@@@@@@@M89A;.8S.       ,r3@@@@@@@@@@@@@@@@@@@@@@@@@@@r    
--   ,13B@@@@@@@@@@@@@@@@@@@5 5B3 ;.         ;@@@@@@@@@@@@@@@@@@@@@@@@@@@i    
--  5#@@#&@@@@@@@@@@@@@@@@@@9  .39:          ;@@@@@@@@@@@@@@@@@@@@@@@@@@@;    
--  9@@@X:MM@@@@@@@@@@@@@@@#;    ;31.         H@@@@@@@@@@@@@@@@@@@@@@@@@@:    
--   SH#@B9.rM@@@@@@@@@@@@@B       :.         3@@@@@@@@@@@@@@@@@@@@@@@@@@5    
--     ,:.   9@@@@@@@@@@@#HB5                 .M@@@@@@@@@@@@@@@@@@@@@@@@@B    
--           ,ssirhSM@&1;i19911i,.             s@@@@@@@@@@@@@@@@@@@@@@@@@@S   
--              ,,,rHAri1h1rh&@#353Sh:          8@@@@@@@@@@@@@@@@@@@@@@@@@#:  
--            .A3hH@#5S553&@@#h   i:i9S          #@@@@@@@@@@@@@@@@@@@@@@@@@A.
-- 
-- 
--    又看源码，看你妹妹呀！
-- -- 1.3 行为标签-用户活跃信息-收看CCTV频道标签表（当日/当月）






INSERT OVERWRITE TABLE knowyou_ott_dmt. PARTITION(dt = '${C_DAY}')
SELECT
    device_id,      user_sub_id,
            
FROM
(
    SELECT
        COALESCE(a.device_id,b.device_id,c.device_id,d.device_id,e.device_id,f.device_id) device_id,       

    FROM
    (
        SELECT * FROM knowyou_ott_dmt. 
        WHERE dt = '${C_DAY}'
    ) a
    FULL JOIN
    (
        SELECT * FROM knowyou_ott_dmt.
        WHERE dt = '${C_DAY}'
    ) b ON a.device_id = b.device_id 
    FULL JOIN
    (
        SELECT * FROM knowyou_ott_dmt.
        WHERE dt = '${C_DAY}'
    ) c ON a.device_id = c.device_id 
    FULL JOIN
    (
        SELECT * FROM knowyou_ott_dmt. 
        WHERE dt = '${C_DAY}'
    ) d ON a.device_id = d.device_id 
    FULL JOIN
    (
        SELECT * FROM knowyou_ott_dmt.
        WHERE dt = '${C_DAY}'
    ) e ON a.device_id = e.device_id 
    FULL JOIN
    (
        SELECT * FROM knowyou_ott_dmt.
        WHERE dt = '${C_DAY}'
    ) f ON a.device_id = f.device_id 
) t1
LEFT JOIN
(
    SELECT user_sub_id, mac FROM
    (
        SELECT user_sub_id, mac, row_number() over(partition by mac order by active_time desc) rn
        FROM knowyou_ott_ods.dwd_jf_user_itv_df
        WHERE dt = '${C_DAY}' AND length(mac) = 17
    ) t WHERE rn = 1
) t2 ON lower(substr(t1.device_id, -12, 12)) = lower(REGEXP_REPLACE(t2.mac, ':', ''))
;
 
--
--                 .-~~~~~~~~~-._       _.-~~~~~~~~~-.
--             __.'              ~.   .~              `.__
--           .'//                  \./                  \\`.
--         .'//                     |                     \\`.
--       .'// .-~"""""""~~~~-._     |     _,-~~~~"""""""~-. \\`.
--     .'//.-"                 `-.  |  .-'                 "-.\\`.
--   .'//______.============-..   \ | /   ..-============.______\\`.
-- .'______________________________\|/______________________________`.
--
--
-- 1.4 行为标签-用户活跃信息-收看点播栏目标签表（当日/当月）
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_demAND_type_watched_part1 PARTITION(dt = '${C_DAY}')
SELECT  
    a.deviceid AS device_id,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS type_tv_is_watched,
    (CASE WHEN c.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS type_film_is_watched,
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS type_child_is_watched,
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS type_comic_is_watched,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS type_sport_is_watched,
    (CASE WHEN g.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS type_doc_is_watched,
    (CASE WHEN h.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS type_edu_is_watched,
    (CASE WHEN i.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS type_game_is_watched,
    (CASE WHEN j.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS type_espo_is_watched,
    (CASE WHEN k.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS type_var_is_watched,
    (CASE WHEN l.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS type_life_is_watched
FROM
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt = '${C_DAY}'
    GROUP BY deviceid
) a
LEFT JOIN
( 
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt = '${C_DAY}' AND contentType = '电视剧' AND videotype = '点播'
    GROUP BY deviceid
) b ON a.deviceid = b.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt = '${C_DAY}' AND contentType = '电影' AND videotype = '点播'
    GROUP BY deviceid
) c ON a.deviceid = c.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt = '${C_DAY}' AND contentType = '少儿' AND videotype = '点播'
    GROUP BY deviceid
) d ON a.deviceid = d.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt = '${C_DAY}' AND contentType = '动漫' AND videotype = '点播'
    GROUP BY deviceid
) e ON a.deviceid = e.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt = '${C_DAY}' AND contentType = '体育' AND videotype = '点播'
    GROUP BY deviceid
) f ON a.deviceid = f.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt = '${C_DAY}' AND contentType = '纪实' AND videotype = '点播'
    GROUP BY deviceid
) g ON a.deviceid = g.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt = '${C_DAY}' AND contentType = '教育' AND videotype = '点播'
    GROUP BY deviceid
) h ON a.deviceid = h.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt = '${C_DAY}' AND contentType = '游戏' AND videotype = '点播'
    GROUP BY deviceid
) i ON a.deviceid = i.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt ='${C_DAY}' AND contentType = '电竞' AND videotype = '点播'
    GROUP BY deviceid
) j ON a.deviceid=j.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt = '${C_DAY}' AND contentType = '综艺' AND videotype = '点播'
    GROUP BY deviceid
) k ON a.deviceid = k.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt = '${C_DAY}' AND contentType = '生活' AND videotype = '点播'
    GROUP BY deviceid
) l ON a.deviceid = l.deviceid 
;


INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_demAND_type_watched_part2 PARTITION(dt = '${C_DAY}')
SELECT  
    a.deviceid AS device_id,
    (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_type_tv_is_watched,
    (CASE WHEN c.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_type_film_is_watched,
    (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_type_child_is_watched,
    (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_type_comic_is_watched,
    (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_type_sport_is_watched,
    (CASE WHEN g.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_type_doc_is_watched,
    (CASE WHEN h.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_type_edu_is_watched,
    (CASE WHEN i.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_type_game_is_watched,
    (CASE WHEN j.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_type_espo_is_watched,
    (CASE WHEN k.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_type_var_is_watched,
    (CASE WHEN l.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS month_type_life_is_watched
FROM
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01'
    GROUP BY deviceid
) a
LEFT JOIN
( 
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND contentType = '电视剧' AND videotype = '点播'
    GROUP BY deviceid
) b ON a.deviceid = b.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND contentType = '电影' AND videotype = '点播'
    GROUP BY deviceid
) c ON a.deviceid = c.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND contentType = '少儿' AND videotype = '点播'
    GROUP BY deviceid
) d ON a.deviceid = d.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND contentType = '动漫' AND videotype = '点播'
    GROUP BY deviceid
) e ON a.deviceid = e.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND contentType = '体育' AND videotype = '点播'
    GROUP BY deviceid
) f ON a.deviceid = f.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND contentType = '纪实' AND videotype = '点播'
    GROUP BY deviceid
) g ON a.deviceid = g.deviceid
LEFT JOIN(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND contentType = '教育' AND videotype = '点播'
    GROUP BY deviceid
) h ON a.deviceid = h.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND contentType = '游戏' AND videotype = '点播'
    GROUP BY deviceid
) i ON a.deviceid = i.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND contentType = '电竞' AND videotype = '点播'
    GROUP BY deviceid
) j ON a.deviceid=j.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND contentType = '综艺' AND videotype = '点播'
    GROUP BY deviceid
) k ON a.deviceid = k.deviceid
LEFT JOIN
(
    SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND contentType = '生活' AND videotype = '点播'
    GROUP BY deviceid
) l ON a.deviceid = l.deviceid 
;


INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_demAND_type_watched_total PARTITION(dt = '${C_DAY}')
SELECT
    device_id,                                                                         user_sub_id,
    NVL(type_tv_is_watched,0),       NVL(type_film_is_watched,0),     NVL(type_child_is_watched,0),
    NVL(type_comic_is_watched,0),    NVL(type_sport_is_watched,0),    NVL(type_doc_is_watched,0),
    NVL(type_edu_is_watched,0),      NVL(type_game_is_watched,0),     NVL(type_espo_is_watched,0),
    NVL(type_var_is_watched,0),      NVL(type_life_is_watched,0),
    month_type_tv_is_watched,        month_type_film_is_watched,      month_type_child_is_watched,
    month_type_comic_is_watched,     month_type_sport_is_watched,     month_type_doc_is_watched,
    month_type_edu_is_watched,       month_type_game_is_watched,      month_type_espo_is_watched,
    month_type_var_is_watched,       month_type_life_is_watched        
FROM
(
    SELECT
        COALESCE(a.device_id, b.device_id) device_id,       
        type_tv_is_watched,     type_film_is_watched,  type_child_is_watched,  type_comic_is_watched,
        type_sport_is_watched,  type_doc_is_watched,   type_edu_is_watched,    type_game_is_watched,
        type_espo_is_watched,   type_var_is_watched,   type_life_is_watched,        
        month_type_tv_is_watched,     month_type_film_is_watched,    month_type_child_is_watched,
        month_type_comic_is_watched,  month_type_sport_is_watched,   month_type_doc_is_watched,
        month_type_edu_is_watched,    month_type_game_is_watched,    month_type_espo_is_watched,
        month_type_var_is_watched,    month_type_life_is_watched

    FROM
    (
        SELECT * FROM knowyou_ott_dmt.htv_demAND_type_watched_part2 
        WHERE dt = '${C_DAY}'
    ) a
    FULL JOIN
    (
        SELECT * FROM knowyou_ott_dmt.htv_demAND_type_watched_part1
        WHERE dt = '${C_DAY}'
    ) b ON a.device_id = b.device_id 
) t1
LEFT JOIN
(
    SELECT user_sub_id, mac FROM
    (
        SELECT user_sub_id, mac, row_number() over(partition by mac order by active_time desc) rn
        FROM knowyou_ott_ods.dwd_jf_user_itv_df
        WHERE dt = '${C_DAY}' AND length(mac) = 17
    ) t WHERE rn = 1
) t2 ON lower(substr(t1.device_id, -12, 12)) = lower(REGEXP_REPLACE(t2.mac, ':', ''))
;

--
--
--    
--     _.'__    `.
--      .--($)($$)---/#\
--    .' @          /###\
--    :         ,   #####
--     `-..__.-' _.-\###/
--           `;_:    `"'
--         .'"""""`.
--        /,  ya ,\\
--       //  404!  \\
--       `-._______.-'
--       ___`. | .'___
--      (______|______)
--   
--
-- 1.5 行为标签-用户回流信息-7日、14日、30日、上月回流用户
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_reflow_user PARTITION(dt = '${C_DAY}')
SELECT
    device_id,          user_sub_id,
    pre7_reflow_user,   pre14_reflow_user,
    pre30_reflow_user,  pre_month_reflow_user            
FROM
(
    SELECT
        a.deviceid device_id,
        (CASE WHEN boot_status_cur = 1 AND NVL(boot_status_7 ,0) = 0 THEN 1 ELSE 0 END) pre7_reflow_user,
        (CASE WHEN boot_status_cur = 1 AND NVL(boot_status_14,0) = 0 THEN 1 ELSE 0 END) pre14_reflow_user,
        (CASE WHEN boot_status_cur = 1 AND NVL(boot_status_30,0) = 0 THEN 1 ELSE 0 END) pre30_reflow_user,
        (CASE WHEN boot_status_cur = 1 AND NVL(boot_status_pre_month,0) = 0 THEN 1 ELSE 0 END) pre_month_reflow_user
    FROM
    (
        SELECT deviceid, MAX(bootstatusday) boot_status_cur
        FROM knowyou_ott_ods.dim_mbh_user_itv_df 
        WHERE dt='${C_DAY}'  
        GROUP BY deviceid
    ) a
    LEFT JOIN 
    (
        SELECT deviceid, MAX(bootstatusday) boot_status_7
        FROM knowyou_ott_ods.dim_mbh_user_itv_df 
        WHERE dt >= '${SEVEN_DAY}' AND dt <= '${Y_DAY}' 
        GROUP BY deviceid
    ) b ON a.deviceid = b.deviceid
    LEFT JOIN
    (
        SELECT deviceid, MAX(bootstatusday) boot_status_14
        FROM knowyou_ott_ods.dim_mbh_user_itv_df 
        WHERE dt >= '${FOURTEEN_DAY}' AND dt <= '${Y_DAY}' 
        GROUP BY deviceid
    ) c ON a.deviceid = c.deviceid
    LEFT JOIN
    (
        SELECT deviceid, MAX(bootstatusday) boot_status_30
        FROM knowyou_ott_ods.dim_mbh_user_itv_df 
        WHERE dt >= '${THIRTY_DAY}' AND dt <= '${Y_DAY}' 
        GROUP BY deviceid
    ) d ON a.deviceid = d.deviceid
    LEFT JOIN
    (
        SELECT deviceid, MAX(bootstatusday) boot_status_pre_month
        FROM knowyou_ott_ods.dim_mbh_user_itv_df 
        WHERE dt >= '${SIXTY_DAY}' AND dt <= '${THIRTY_DAY}' 
        GROUP BY deviceid
    ) e ON a.deviceid = e.deviceid
) t1
LEFT JOIN
(
    SELECT user_sub_id, mac FROM
    (
        SELECT user_sub_id, mac, row_number() over(partition by mac order by active_time desc) rn
        FROM knowyou_ott_ods.dwd_jf_user_itv_df
        WHERE dt = '${C_DAY}' AND length(mac) = 17
    ) t WHERE rn = 1
) t2 ON lower(substr(t1.device_id, -12, 12)) = lower(REGEXP_REPLACE(t2.mac, ':', ''))
;



-- 1.6 行为标签-用户沉默信息-7日、14日、30日、上月沉默用户
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_silence_user PARTITION(dt = '${C_DAY}')
SELECT
    device_id,           user_sub_id,
    pre7_silence_user,   pre14_silence_user,
    pre30_silence_user,  pre_month_silence_user            
FROM
(
    SELECT
        a.deviceid device_id,
        (CASE WHEN NVL(boot_status_7 ,0) = 0 THEN 1 ELSE 0 END) pre7_silence_user,
        (CASE WHEN NVL(boot_status_14,0) = 0 THEN 1 ELSE 0 END) pre14_silence_user,
        (CASE WHEN NVL(boot_status_30,0) = 0 THEN 1 ELSE 0 END) pre30_silence_user,
        (CASE WHEN NVL(boot_status_pre_month,0) = 0 THEN 1 ELSE 0 END) pre_month_silence_user
    FROM
    (
        SELECT deviceid
        FROM knowyou_ott_ods.dim_mbh_user_itv_df 
        WHERE dt='${C_DAY}'  
        GROUP BY deviceid
    ) a
    LEFT JOIN 
    (
        SELECT deviceid, MAX(bootstatusday) boot_status_7
        FROM knowyou_ott_ods.dim_mbh_user_itv_df 
        WHERE dt >= '${SEVEN_DAY}' AND dt <= '${C_DAY}' 
        GROUP BY deviceid
    ) b ON a.deviceid = b.deviceid
    LEFT JOIN
    (
        SELECT deviceid, MAX(bootstatusday) boot_status_14
        FROM knowyou_ott_ods.dim_mbh_user_itv_df 
        WHERE dt >= '${FOURTEEN_DAY}' AND dt <= '${C_DAY}' 
        GROUP BY deviceid
    ) c ON a.deviceid = c.deviceid
    LEFT JOIN
    (
        SELECT deviceid, MAX(bootstatusday) boot_status_30
        FROM knowyou_ott_ods.dim_mbh_user_itv_df 
        WHERE dt >= '${THIRTY_DAY}' AND dt <= '${C_DAY}' 
        GROUP BY deviceid
    ) d ON a.deviceid = d.deviceid
    LEFT JOIN
    (
        SELECT deviceid, MAX(bootstatusday) boot_status_pre_month
        FROM knowyou_ott_ods.dim_mbh_user_itv_df 
        WHERE dt >= '${SIXTY_DAY}' AND dt <= '${THIRTY_DAY}' 
        GROUP BY deviceid
    ) e ON a.deviceid = e.deviceid
) t1
LEFT JOIN
(
    SELECT user_sub_id, mac FROM
    (
        SELECT user_sub_id, mac, row_number() over(partition by mac order by active_time desc) rn
        FROM knowyou_ott_ods.dwd_jf_user_itv_df
        WHERE dt = '${C_DAY}' AND length(mac) = 17
    ) t WHERE rn = 1
) t2 ON lower(substr(t1.device_id, -12, 12)) = lower(REGEXP_REPLACE(t2.mac, ':', ''))
;


-- —————— /´ ¯/)
-- —————–/—-/
-- —————-/—-/
-- ———–/´¯/’–’/´¯`·_
-- ———-/’/–/—-/—–/¨¯
-- ——–(’(———- ¯~/’–’)
-- ———\————-’—–/
-- ———-’’————_-·´
-- ————\———–(
-- ————-\———-- 小小中指，不成敬意。
-- 1.7 行为标签-收视偏好-类别偏好-各点播类别（电影、电视剧、综艺、少儿）的轻、中、重偏好
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_demAND_type_prefer_total PARTITION(dt = '${C_DAY}')
SELECT
    device_id,           user_sub_id,
    film_prefer_light,   film_prefer_middle,    film_prefer_high,
    tv_prefer_light,     tv_prefer_middle,      tv_prefer_high,
    var_prefer_light,    var_prefer_middle,     var_prefer_high,
    child_prefer_light,  child_prefer_middle,   child_prefer_high          
FROM
(
    SELECT  
        device_id,
        CASE WHEN film < 3600 THEN 1 ELSE 0 END film_prefer_light,
        CASE WHEN film >= 3600 AND film <= 10800 THEN 1 ELSE 0 END film_prefer_middle,
        CASE WHEN film > 10800 THEN 1 ELSE 0 END film_prefer_high,
        CASE WHEN tv < 2700 THEN 1 ELSE 0 END tv_prefer_light,
        CASE WHEN tv >= 2700 AND tv <= 5400 THEN 1 ELSE 0 END tv_prefer_middle,
        CASE WHEN tv > 5400 THEN 1 ELSE 0 END tv_prefer_high,
        CASE WHEN var < 2700 THEN 1 ELSE 0 END var_prefer_light,
        CASE WHEN var >= 2700 AND var <= 5400 THEN 1 ELSE 0 END var_prefer_middle,
        CASE WHEN var > 5400 THEN 1 ELSE 0 END var_prefer_high,
        CASE WHEN child < 2700 THEN 1 ELSE 0 END child_prefer_light,
        CASE WHEN child >= 2700 AND child <= 5400 THEN 1 ELSE 0 END child_prefer_middle,
        CASE WHEN child > 5400 THEN 1 ELSE 0 END child_prefer_high    
    FROM
    (
        SELECT 
            coalesce(a.deviceid, b.deviceid, c.deviceid, d.deviceid) device_id,
            NVL(a.play_duration,0) film,
            NVL(b.play_duration,0) tv,
            NVL(c.play_duration,0) var,
            NVL(d.play_duration,0) child
        FROM
        (
            SELECT deviceid, sum(playTime) AS play_duration 
            FROM knowyou_ott_ods.dws_all_video_playinfo_di
            WHERE dt ='${C_DAY}' AND contentType = '电影' AND videotype = '点播'
            GROUP BY deviceid
        ) a 
        FULL JOIN
        (
            SELECT deviceid, sum(playTime) AS play_duration 
            FROM knowyou_ott_ods.dws_all_video_playinfo_di
            WHERE dt ='${C_DAY}' AND contentType = '电视剧' AND videotype = '点播'
            GROUP BY deviceid
        ) b ON a.deviceid = b.deviceid
        FULL JOIN
        (
            SELECT deviceid, sum(playTime) AS play_duration 
            FROM knowyou_ott_ods.dws_all_video_playinfo_di
            WHERE dt ='${C_DAY}' AND contentType = '综艺' AND videotype = '点播'
            GROUP BY deviceid
        ) c ON a.deviceid = c.deviceid
        FULL JOIN
        (
            SELECT deviceid, sum(playTime) AS play_duration 
            FROM knowyou_ott_ods.dws_all_video_playinfo_di
            WHERE dt ='${C_DAY}' AND contentType = '少儿' AND videotype = '点播'
            GROUP BY deviceid
        ) d ON a.deviceid = d.deviceid
    ) t
) t1
LEFT JOIN
(
    SELECT user_sub_id, mac FROM
    (
        SELECT user_sub_id, mac, row_number() over(partition by mac order by active_time desc) rn
        FROM knowyou_ott_ods.dwd_jf_user_itv_df
        WHERE dt = '${C_DAY}' AND length(mac) = 17
    ) t WHERE rn = 1
) t2 ON lower(substr(t1.device_id, -12, 12)) = lower(REGEXP_REPLACE(t2.mac, ':', ''))
;


-- 1.8 行为标签-收视偏好-内容偏好-各点播类别（电影、电视剧、综艺、少儿）1000个热门内容的轻、中、重偏好
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_demAND_film_content_prefer PARTITION(dt = '${C_DAY}')
SELECT
    device_id,                   user_sub_id,                   programname,
    film_content_prefer_light,   film_content_prefer_middle,    film_content_prefer_high         
FROM
(
    SELECT 
        deviceid device_id,
        t1.programname,
        CASE WHEN play_duration < 2700 THEN 1 ELSE 0 END film_content_prefer_light,
        CASE WHEN play_duration >= 2700 AND play_duration <= 5400 THEN 1 ELSE 0 END film_content_prefer_middle,
        CASE WHEN play_duration > 5400 THEN 1 ELSE 0 END film_content_prefer_high
    FROM
    (
        SELECT deviceid, programname, sum(playTime) AS play_duration 
        FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt ='${C_DAY}' AND contentType = '电影' AND videotype = '点播'
        GROUP BY deviceid, programname
    ) t1
    LEFT JOIN 
    (
        SELECT programname FROM
        (
            SELECT programname, row_number() over(partition by 0 order by cnt desc) rn
            FROM
            (
                SELECT programname, sum(playtime) cnt
                FROM knowyou_ott_ods.dws_all_video_playinfo_di
                WHERE dt ='${C_DAY}' AND contentType = '电影' AND videotype = '点播'
                GROUP BY programname
            ) a
        ) b 
        WHERE rn <= 1000
    ) t2 ON t1.programname = t2.programname
) t1
LEFT JOIN
(
    SELECT user_sub_id, mac FROM
    (
        SELECT user_sub_id, mac, row_number() over(partition by mac order by active_time desc) rn
        FROM knowyou_ott_ods.dwd_jf_user_itv_df
        WHERE dt = '${C_DAY}' AND length(mac) = 17
    ) t WHERE rn = 1
) t2 ON lower(substr(t1.device_id, -12, 12)) = lower(REGEXP_REPLACE(t2.mac, ':', ''))
;


INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_demAND_tv_content_prefer PARTITION(dt = '${C_DAY}')
SELECT
    device_id,                user_sub_id,                programname, 
    tv_content_prefer_light,  tv_content_prefer_middle,   tv_content_prefer_high         
FROM
(
    SELECT 
        deviceid device_id,
        t1.programname,
        CASE WHEN play_duration < 2700 THEN 1 ELSE 0 END tv_content_prefer_light,
        CASE WHEN play_duration >= 2700 AND play_duration <= 5400 THEN 1 ELSE 0 END tv_content_prefer_middle,
        CASE WHEN play_duration > 5400 THEN 1 ELSE 0 END tv_content_prefer_high
    FROM
    (
        SELECT deviceid, programname, sum(playTime) AS play_duration 
        FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt ='${C_DAY}' AND contentType = '电视剧' AND videotype = '点播'
        GROUP BY deviceid, programname
    ) t1
    LEFT JOIN 
    (
        SELECT programname FROM
        (
            SELECT programname, row_number() over(partition by 0 order by cnt desc) rn
            FROM
            (
                SELECT programname, sum(playtime) cnt
                FROM knowyou_ott_ods.dws_all_video_playinfo_di
                WHERE dt ='${C_DAY}' AND contentType = '电视剧' AND videotype = '点播'
                GROUP BY programname
            ) a
        ) b 
        WHERE rn <= 1000
    ) t2 ON t1.programname = t2.programname
) t1
LEFT JOIN
(
    SELECT user_sub_id, mac FROM
    (
        SELECT user_sub_id, mac, row_number() over(partition by mac order by active_time desc) rn
        FROM knowyou_ott_ods.dwd_jf_user_itv_df
        WHERE dt = '${C_DAY}' AND length(mac) = 17
    ) t WHERE rn = 1
) t2 ON lower(substr(t1.device_id, -12, 12)) = lower(REGEXP_REPLACE(t2.mac, ':', ''))
;


INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_demAND_var_content_prefer PARTITION(dt = '${C_DAY}')
SELECT
    device_id,                   user_sub_id,                   programname,
    var_content_prefer_light,    var_content_prefer_middle,     var_content_prefer_high        
FROM
(
    SELECT 
        deviceid device_id,
        t1.programname,
        CASE WHEN play_duration < 2700 THEN 1 ELSE 0 END var_content_prefer_light,
        CASE WHEN play_duration >= 2700 AND play_duration <= 5400 THEN 1 ELSE 0 END var_content_prefer_middle,
        CASE WHEN play_duration > 5400 THEN 1 ELSE 0 END var_content_prefer_high
    FROM
    (
        SELECT deviceid, programname, sum(playTime) AS play_duration 
        FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt ='${C_DAY}' AND contentType = '综艺' AND videotype = '点播'
        GROUP BY deviceid, programname
    ) t1
    LEFT JOIN 
    (
        SELECT programname FROM
        (
            SELECT programname, row_number() over(partition by 0 order by cnt desc) rn
            FROM
            (
                SELECT programname, sum(playtime) cnt
                FROM knowyou_ott_ods.dws_all_video_playinfo_di
                WHERE dt ='${C_DAY}' AND contentType = '综艺' AND videotype = '点播'
                GROUP BY programname
            ) a
        ) b 
        WHERE rn <= 1000
    ) t2 ON t1.programname = t2.programname
) t1
LEFT JOIN
(
    SELECT user_sub_id, mac FROM
    (
        SELECT user_sub_id, mac, row_number() over(partition by mac order by active_time desc) rn
        FROM knowyou_ott_ods.dwd_jf_user_itv_df
        WHERE dt = '${C_DAY}' AND length(mac) = 17
    ) t WHERE rn = 1
) t2 ON lower(substr(t1.device_id, -12, 12)) = lower(REGEXP_REPLACE(t2.mac, ':', ''))
;


INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_demAND_child_content_prefer PARTITION(dt = '${C_DAY}')
SELECT
    device_id,                   user_sub_id,                   programname,
    child_content_prefer_light,  child_content_prefer_middle,   child_content_prefer_high          
FROM
(
    SELECT 
        deviceid device_id,
        t1.programname,
        CASE WHEN play_duration < 2700 THEN 1 ELSE 0 END child_content_prefer_light,
        CASE WHEN play_duration >= 2700 AND play_duration <= 5400 THEN 1 ELSE 0 END child_content_prefer_middle,
        CASE WHEN play_duration > 5400 THEN 1 ELSE 0 END child_content_prefer_high
    FROM
    (
        SELECT deviceid, programname, sum(playTime) AS play_duration 
        FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt ='${C_DAY}' AND contentType = '少儿' AND videotype = '点播'
        GROUP BY deviceid, programname
    ) t1
    LEFT JOIN 
    (
        SELECT programname FROM
        (
            SELECT programname, row_number() over(partition by 0 order by cnt desc) rn
            FROM
            (
                SELECT programname, sum(playtime) cnt
                FROM knowyou_ott_ods.dws_all_video_playinfo_di
                WHERE dt ='${C_DAY}' AND contentType = '少儿' AND videotype = '点播'
                GROUP BY programname
            ) a
        ) b 
        WHERE rn <= 1000
    ) t2 ON t1.programname = t2.programname
) t1
LEFT JOIN
(
    SELECT user_sub_id, mac FROM
    (
        SELECT user_sub_id, mac, row_number() over(partition by mac order by active_time desc) rn
        FROM knowyou_ott_ods.dwd_jf_user_itv_df
        WHERE dt = '${C_DAY}' AND length(mac) = 17
    ) t WHERE rn = 1
) t2 ON lower(substr(t1.device_id, -12, 12)) = lower(REGEXP_REPLACE(t2.mac, ':', ''))
;



-- 1.9 行为标签-收视偏好-内容属性-当日是否看过xx类型内容, 不区分点直回
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_type_prefer_total PARTITION(dt = '${C_DAY}') 
SELECT
    device_id,                  user_sub_id,                   
    dlr_type_tv_is_watched,     dlr_type_film_is_watched,   dlr_type_var_is_watched,
    dlr_type_child_is_watched,  dlr_type_web_is_watched,    dlr_type_micro_is_watched,
    dlr_type_short_is_watched,  dlr_type_comic_is_watched,  dlr_type_sport_is_watched,
    dlr_type_doc_is_watched,    dlr_type_edu_is_watched,    dlr_type_esport_is_watched,
    dlr_type_game_is_watched,   dlr_type_music_is_watched,  dlr_type_enter_is_watched,
    dlr_type_drama_is_watched,  dlr_type_life_is_watched         
FROM
(
    SELECT  
        a.deviceid device_id,
        (CASE WHEN b.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS dlr_type_tv_is_watched,
        (CASE WHEN c.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS dlr_type_film_is_watched,
        (CASE WHEN d.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS dlr_type_var_is_watched,
        (CASE WHEN e.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS dlr_type_child_is_watched,
        (CASE WHEN f.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS dlr_type_web_is_watched,
        (CASE WHEN g.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS dlr_type_micro_is_watched,
        (CASE WHEN h.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS dlr_type_short_is_watched,
        (CASE WHEN i.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS dlr_type_comic_is_watched,
        (CASE WHEN j.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS dlr_type_sport_is_watched,
        (CASE WHEN k.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS dlr_type_doc_is_watched,
        (CASE WHEN l.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS dlr_type_edu_is_watched,
        (CASE WHEN m.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS dlr_type_esport_is_watched,
        (CASE WHEN n.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS dlr_type_game_is_watched,
        (CASE WHEN o.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS dlr_type_music_is_watched,
        (CASE WHEN p.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS dlr_type_enter_is_watched,
        (CASE WHEN q.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS dlr_type_drama_is_watched,
        (CASE WHEN r.deviceid IS NOT NULL THEN 1 ELSE 0 END) AS dlr_type_life_is_watched
    FROM
    (
        SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt = '${C_DAY}'
        GROUP BY deviceid
    ) a
    LEFT JOIN
    ( 
        SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt = '${C_DAY}' AND contentType = '电视剧'
        GROUP BY deviceid
    ) b ON a.deviceid = b.deviceid
    LEFT JOIN
    ( 
        SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt = '${C_DAY}' AND contentType = '电影'
        GROUP BY deviceid
    ) c ON a.deviceid = c.deviceid
    LEFT JOIN
    (
        SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt = '${C_DAY}' AND contentType = '综艺' 
        GROUP BY deviceid
    ) d ON a.deviceid = d.deviceid
    LEFT JOIN
    (
        SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt = '${C_DAY}' AND contentType = '少儿' 
        GROUP BY deviceid
    ) e ON a.deviceid = e.deviceid
    LEFT JOIN
    (
        SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt = '${C_DAY}' AND contentType = '网剧' 
        GROUP BY deviceid
    ) f ON a.deviceid = f.deviceid
    LEFT JOIN
    (
        SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt = '${C_DAY}' AND contentType = '微电影' 
    ) g ON a.deviceid = g.deviceid
    LEFT JOIN
    (
        SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt = '${C_DAY}' AND contentType = '短视频' 
        GROUP BY deviceid
    ) h ON a.deviceid = h.deviceid
    LEFT JOIN
    (
        SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt = '${C_DAY}' AND contentType = '动漫' 
        GROUP BY deviceid
    ) i ON a.deviceid = i.deviceid
    LEFT JOIN
    (
        SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt = '${C_DAY}' AND contentType = '体育' 
        GROUP BY deviceid
    ) j ON a.deviceid = j.deviceid
    LEFT JOIN
    (
        SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt = '${C_DAY}' AND contentType = '纪实' 
        GROUP BY deviceid
    ) k ON a.deviceid = k.deviceid
    LEFT JOIN
    (
        SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt = '${C_DAY}' AND contentType = '教育' 
        GROUP BY deviceid
    ) l ON a.deviceid = l.deviceid 
    LEFT JOIN
    (
        SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt = '${C_DAY}' AND contentType = '电竞' 
        GROUP BY deviceid
    ) m ON a.deviceid = m.deviceid 
    LEFT JOIN
    (
        SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt = '${C_DAY}' AND contentType = '游戏' 
        GROUP BY deviceid
    ) n ON a.deviceid = n.deviceid 
    LEFT JOIN
    (
        SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt = '${C_DAY}' AND contentType = '音乐' 
        GROUP BY deviceid
    ) o ON a.deviceid = o.deviceid 
    LEFT JOIN
    (
        SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt = '${C_DAY}' AND contentType = '娱乐' 
        GROUP BY deviceid
    ) p ON a.deviceid = p.deviceid 
    LEFT JOIN
    (
        SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt = '${C_DAY}' AND contentType = '戏曲' 
        GROUP BY deviceid
    ) q ON a.deviceid = q.deviceid 
    LEFT JOIN
    (
        SELECT deviceid FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt = '${C_DAY}' AND contentType = '生活' 
        GROUP BY deviceid
    ) r ON a.deviceid = r.deviceid 
) t1
LEFT JOIN
(
    SELECT user_sub_id, mac FROM
    (
        SELECT user_sub_id, mac, row_number() over(partition by mac order by active_time desc) rn
        FROM knowyou_ott_ods.dwd_jf_user_itv_df
        WHERE dt = '${C_DAY}' AND length(mac) = 17
    ) t WHERE rn = 1
) t2 ON lower(substr(t1.device_id, -12, 12)) = lower(REGEXP_REPLACE(t2.mac, ':', ''))
;


-- *
--                    _ooOoo_
--                   o8888888o
--                   88" . "88
--                   (| -_- |)
--                    O\ = /O
--                ____/`---'\____
--              .   ' \\| |// `.
--               / \\||| : |||// \
--             / _||||| -:- |||||- \
--               | | \\\ - /// | |
--             | \_| ''\---/'' | |
--              \ .-\__ `-` ___/-. /
--           ___`. .' /--.--\ `. . __
--        ."" '< `.___\_<|>_/___.' >'"".
--       | | : `- \`.;`\ _ /`;.`/ - ` : | |
--         \ \ `-. \_ __\ /__ _/ .-` / /
-- ======`-.____`-.___\_____/___.-`____.-'======
--                    `=---='
-- 
-- .............................................
--          佛祖保佑             永无BUG
-- 
-- 1.10 行为标签-行为标签-用户搜索-搜索热词TOP10（当日/当月）
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_search_collect_top10_total PARTITION(dt = '${C_DAY}')
SELECT
    tag,  content, device_id,  user_sub_id          
FROM
(
    -- search
    SELECT 
        deviceid device_id,
        t1.content content,
        tag
    FROM
    (
        SELECT deviceid, programname content
        FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt ='${C_DAY}' AND  entrytype ='搜索' AND programname !='' AND programname regexp '[\\u4E00-\\u9FFF]+' 
    ) t1
    JOIN
    (
        SELECT content, concat('search_top', rn, '_day') tag FROM
        (
            SELECT content, cnt, row_number() over(partition by 0 order by cnt desc) rn
            FROM
            (
                SELECT programname content, count(distinct deviceid) cnt
                FROM knowyou_ott_ods.dws_all_video_playinfo_di
                WHERE dt ='${C_DAY}' AND  entrytype ='搜索' AND programname !='' AND programname regexp '[\\u4E00-\\u9FFF]+' 
                GROUP BY programname
            ) a
        ) b 
        WHERE rn <= 10
    ) t2 ON t1.content = t2.content

    UNION ALL

    SELECT 
        deviceid device_id,
        t1.content content,
        tag
    FROM
    (
        SELECT deviceid, programname content
        FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND  entrytype ='搜索' AND programname !='' AND programname regexp '[\\u4E00-\\u9FFF]+' 
    ) t1
    JOIN
    (
        SELECT content, concat('search_top', rn, '_month') tag FROM
        (
            SELECT content, cnt, row_number() over(partition by 0 order by cnt desc) rn
            FROM
            (
                SELECT programname content, count(distinct deviceid) cnt
                FROM knowyou_ott_ods.dws_all_video_playinfo_di
                WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND  entrytype ='搜索' AND programname !='' AND programname regexp '[\\u4E00-\\u9FFF]+' 
                GROUP BY programname
            ) a
        ) b 
        WHERE rn <= 10
    ) t2 ON t1.content = t2.content

    UNION ALL

    -- collect
    SELECT 
        deviceid device_id,
        t1.content content,
        tag
    FROM
    (
        SELECT deviceid, programname content
        FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt ='${C_DAY}' AND  entrytype ='收藏' AND programname !='' AND programname regexp '[\\u4E00-\\u9FFF]+' 
    ) t1
    JOIN
    (
        SELECT content, concat('collect_top', rn, '_day') tag FROM
        (
            SELECT content, cnt, row_number() over(partition by 0 order by cnt desc) rn
            FROM
            (
                SELECT programname content, count(distinct deviceid) cnt
                FROM knowyou_ott_ods.dws_all_video_playinfo_di
                WHERE dt ='${C_DAY}' AND  entrytype ='收藏' AND programname !='' AND programname regexp '[\\u4E00-\\u9FFF]+' 
                GROUP BY programname
            ) a
        ) b 
        WHERE rn <= 10
    ) t2 ON t1.content = t2.content

    UNION ALL

    SELECT 
        deviceid device_id,
        t1.content content,
        tag
    FROM
    (
        SELECT deviceid, programname content
        FROM knowyou_ott_ods.dws_all_video_playinfo_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND  entrytype ='收藏' AND programname !='' AND programname regexp '[\\u4E00-\\u9FFF]+' 
    ) t1
    JOIN
    (
        SELECT content, concat('collect_top', rn, '_month') tag FROM
        (
            SELECT content, cnt, row_number() over(partition by 0 order by cnt desc) rn
            FROM
            (
                SELECT programname content, count(distinct deviceid) cnt
                FROM knowyou_ott_ods.dws_all_video_playinfo_di
                WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND  entrytype ='收藏' AND programname !='' AND programname regexp '[\\u4E00-\\u9FFF]+' 
                GROUP BY programname
            ) a
        ) b 
        WHERE rn <= 10
    ) t2 ON t1.content = t2.content
) t1
LEFT JOIN
(
    SELECT user_sub_id, mac FROM
    (
        SELECT user_sub_id, mac, row_number() over(partition by mac order by active_time desc) rn
        FROM knowyou_ott_ods.dwd_jf_user_itv_df
        WHERE dt = '${C_DAY}' AND length(mac) = 17
    ) t WHERE rn = 1
) t2 ON lower(substr(t1.device_id, -12, 12)) = lower(REGEXP_REPLACE(t2.mac, ':', ''))
;


--                                                                    
--            .,,       .,:;;iiiiiiiii;;:,,.     .,,                   
--          rGB##HS,.;iirrrrriiiiiiiiiirrrrri;,s&##MAS,                
--         r5s;:r3AH5iiiii;;;;;;;;;;;;;;;;iiirXHGSsiih1,               
--            .;i;;s91;;;;;;::::::::::::;;;;iS5;;;ii:                  
--          :rsriii;;r::::::::::::::::::::::;;,;;iiirsi,               
--       .,iri;;::::;;;;;;::,,,,,,,,,,,,,..,,;;;;;;;;iiri,,.           
--    ,9BM&,            .,:;;:,,,,,,,,,,,hXA8:            ..,,,.       
--   ,;&@@#r:;;;;;::::,,.   ,r,,,,,,,,,,iA@@@s,,:::;;;::,,.   .;.      
--    :ih1iii;;;;;::::;;;;;;;:,,,,,,,,,,;i55r;;;;;;;;;iiirrrr,..       
--   .ir;;iiiiiiiiii;;;;::::::,,,,,,,:::::,,:;;;iiiiiiiiiiiiri         
--   iriiiiiiiiiiiiiiii;;;::::::::::::::::;;;iiiiiiiiiiiiiiiir;        
--  ,riii;;;;;;;;;;;;;:::::::::::::::::::::::;;;;;;;;;;;;;;iiir.       
--  iri;;;::::,,,,,,,,,,:::::::::::::::::::::::::,::,,::::;;iir:       
-- .rii;;::::,,,,,,,,,,,,:::::::::::::::::,,,,,,,,,,,,,::::;;iri       
-- ,rii;;;::,,,,,,,,,,,,,:::::::::::,:::::,,,,,,,,,,,,,:::;;;iir.      
-- ,rii;;i::,,,,,,,,,,,,,:::::::::::::::::,,,,,,,,,,,,,,::i;;iir.      
-- ,rii;;r::,,,,,,,,,,,,,:,:::::,:,:::::::,,,,,,,,,,,,,::;r;;iir.      
-- .rii;;rr,:,,,,,,,,,,,,,,:::::::::::::::,,,,,,,,,,,,,:,si;;iri       
--  ;rii;:1i,,,,,,,,,,,,,,,,,,:::::::::,,,,,,,,,,,,,,,:,ss:;iir:       
--  .rii;;;5r,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,sh:;;iri        
--   ;rii;:;51,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,.:hh:;;iir,        
--    irii;::hSr,.,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,.,sSs:;;iir:         
--     irii;;:iSSs:.,,,,,,,,,,,,,,,,,,,,,,,,,,,..:135;:;;iir:          
--      ;rii;;:,r535r:...,,,,,,,,,,,,,,,,,,..,;sS35i,;;iirr:           
--       :rrii;;:,;1S3Shs;:,............,:is533Ss:,;;;iiri,            
--        .;rrii;;;:,;rhS393S55hh11hh5S3393Shr:,:;;;iirr:              
--          .;rriii;;;::,:;is1h555555h1si;:,::;;;iirri:.               
--            .:irrrii;;;;;:::,,,,,,,,:::;;;;iiirrr;,                  
--               .:irrrriiiiii;;;;;;;;iiiiiirrrr;,.                    
--                  .,:;iirrrrrrrrrrrrrrrrri;:.                        
--                        ..,:::;;;;:::,,.                             
--                                                                      
-- 1.11 行为标签-用户订购信息-付费内容订购退订标签（当日/当月）
INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_subscribe_unsubscribe_part1 PARTITION(dt = '${C_DAY}')
SELECT
    b.deviceid device_id,
    b.user_sub_id,
    a.*
FROM
(
    SELECT 
        t0.usercode                           usercode,
        t1.subscribe              bailingkge_subscribe,
        t2.subscribe    chaojiyingshihuiyuan_subscribe,
        t3.subscribe      baiyingyoushenghuo_subscribe,
        t4.subscribe      chaojiyouxihuiyuan_subscribe,
        t5.subscribe   chaojidianjinghuiyuan_subscribe,
        t6.subscribe    jiaoyuhuiyuanyoujiao_subscribe,
        t7.subscribe    jiaoyuhuiyuanxiaoxue_subscribe,
        t8.subscribe   jiaoyuhuiyuanchuzhong_subscribe,
        t9.subscribe   jiaoyuhuiyuangaozhong_subscribe,
        t10.subscribe          yinyuehuiyuan_subscribe,
        t11.subscribe          kumiaohuiyuan_subscribe,
        t12.subscribe             yunshiting_subscribe,
        t13.subscribe            yinheshaoer_subscribe,
        t14.subscribe         shengjianyouxi_subscribe,
        t15.subscribe            migukuaiyou_subscribe,
        t16.subscribe     mengxiangyouxiting_subscribe,
        t17.subscribe          xuanjialeyuan_subscribe,
        t18.subscribe            leyoushijie_subscribe,
        t19.subscribe        dianjingfengbao_subscribe,
        t20.subscribe         touhaodianjing_subscribe,
        t21.subscribe     xingfujianshentuan_subscribe,
        t22.subscribe           jiuzhoulexue_subscribe,
        t23.subscribe                xueersi_subscribe,
        t24.subscribe             dierketang_subscribe,
        t25.subscribe    zhinengyuyinhuiyuan_subscribe,

        t1.unsubscribe              bailingkge_unsubscribe,
        t2.unsubscribe    chaojiyingshihuiyuan_unsubscribe,
        t3.unsubscribe      baiyingyoushenghuo_unsubscribe,
        t4.unsubscribe      chaojiyouxihuiyuan_unsubscribe,
        t5.unsubscribe   chaojidianjinghuiyuan_unsubscribe,
        t6.unsubscribe    jiaoyuhuiyuanyoujiao_unsubscribe,
        t7.unsubscribe    jiaoyuhuiyuanxiaoxue_unsubscribe,
        t8.unsubscribe   jiaoyuhuiyuanchuzhong_unsubscribe,
        t9.unsubscribe   jiaoyuhuiyuangaozhong_unsubscribe,
        t10.unsubscribe          yinyuehuiyuan_unsubscribe,
        t11.unsubscribe          kumiaohuiyuan_unsubscribe,
        t12.unsubscribe             yunshiting_unsubscribe,
        t13.unsubscribe            yinheshaoer_unsubscribe,
        t14.unsubscribe         shengjianyouxi_unsubscribe,
        t15.unsubscribe            migukuaiyou_unsubscribe,
        t16.unsubscribe     mengxiangyouxiting_unsubscribe,
        t17.unsubscribe          xuanjialeyuan_unsubscribe,
        t18.unsubscribe            leyoushijie_unsubscribe,
        t19.unsubscribe        dianjingfengbao_unsubscribe,
        t20.unsubscribe         touhaodianjing_unsubscribe,
        t21.unsubscribe     xingfujianshentuan_unsubscribe,
        t22.unsubscribe           jiuzhoulexue_unsubscribe,
        t23.unsubscribe                xueersi_unsubscribe,
        t24.unsubscribe             dierketang_unsubscribe,
        t25.unsubscribe    zhinengyuyinhuiyuan_unsubscribe
    FROM
    (
        SELECT usercode
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0)
        AND (feename like '%百灵K歌%' OR feename like '%超级影视会员%'OR feename like '%百映优生活%' OR feename like '%超级游戏会员%' OR feename like '%超级电竞会员%' OR feename like '%教育会员-幼教%' OR feename like '%教育会员-小学%' OR feename like '%教育会员-初中%' OR feename like '%教育会员-高中%' OR feename like '%音乐会员%' OR feename like '%酷喵会员%' OR feename like '%云视听%' OR feename like '%银河少儿%' OR feename like '%圣剑游戏%' OR feename like '%咪咕快游%' OR feename like '%梦想游戏厅%' OR feename like '%炫佳乐园%' OR feename like '%乐游世界%' OR feename like '%电竞风暴%' OR feename like '%头号电竞%' OR feename like '%幸福健身团%' OR feename like '%九州乐学%' OR feename like '%学而思%' OR feename like '%第二课堂%' OR feename like '%智能语音会员%' )
        GROUP BY usercode
    ) t0
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%百灵K歌%' 
        GROUP BY usercode, feename
    ) t1 ON t1.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%超级影视会员%'
        GROUP BY usercode, feename
    ) t2 ON t2.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%百映优生活%' 
        GROUP BY usercode, feename
    ) t3 ON t3.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%超级游戏会员%' 
        GROUP BY usercode, feename
    ) t4 ON t4.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%超级电竞会员%' 
        GROUP BY usercode, feename
    ) t5 ON t5.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%教育会员-幼教%' 
        GROUP BY usercode, feename
    ) t6 ON t6.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%教育会员-小学%' 
        GROUP BY usercode, feename
    ) t7 ON t7.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%教育会员-初中%' 
        GROUP BY usercode, feename
    ) t8 ON t8.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%教育会员-高中%' 
        GROUP BY usercode, feename
    ) t9 ON t9.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%音乐会员%' 
        GROUP BY usercode, feename
    ) t10 ON t10.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%酷喵会员%' 
        GROUP BY usercode, feename
    ) t11 ON t11.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%云视听%' 
        GROUP BY usercode, feename
    ) t12 ON t12.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%银河少儿%' 
        GROUP BY usercode, feename
    ) t13 ON t13.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%圣剑游戏%' 
        GROUP BY usercode, feename
    ) t14 ON t14.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%咪咕快游%' 
        GROUP BY usercode, feename
    ) t15 ON t15.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%梦想游戏厅%' 
        GROUP BY usercode, feename
    ) t16 ON t16.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%炫佳乐园%' 
        GROUP BY usercode, feename
    ) t17 ON t17.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%乐游世界%' 
        GROUP BY usercode, feename
    ) t18 ON t18.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%电竞风暴%' 
        GROUP BY usercode, feename
    ) t19 ON t19.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%头号电竞%' 
        GROUP BY usercode, feename
    ) t20 ON t20.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%幸福健身团%' 
        GROUP BY usercode, feename
    ) t21 ON t21.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%九州乐学%' 
        GROUP BY usercode, feename
    ) t22 ON t22.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%学而思%' 
        GROUP BY usercode, feename
    ) t23 ON t23.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%第二课堂%' 
        GROUP BY usercode, feename
    ) t24 ON t24.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt = '${C_DAY}' AND updatetype IN (1,0) AND feename like '%智能语音会员%' 
        GROUP BY usercode, feename
    ) t25 ON t25.usercode = t0.usercode
) a
LEFT JOIN
(
    SELECT 
        deviceid,
        user_id,
        user_sub_id
    FROM knowyou_ott_ods.dim_cus_user_itv_df
    WHERE dt = '${C_DAY}' AND length(deviceid) > 0 AND length(user_id) > 0 
) b ON b.user_id = a.usercode
;


INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_subscribe_unsubscribe_part2 PARTITION(dt ='${C_DAY}')
SELECT
    b.deviceid device_id,
    b.user_sub_id,
    a.*
FROM
(
    SELECT 
        t0.usercode                                 usercode,
        t1.subscribe              month_bailingkge_subscribe,
        t2.subscribe    month_chaojiyingshihuiyuan_subscribe,
        t3.subscribe      month_baiyingyoushenghuo_subscribe,
        t4.subscribe      month_chaojiyouxihuiyuan_subscribe,
        t5.subscribe   month_chaojidianjinghuiyuan_subscribe,
        t6.subscribe    month_jiaoyuhuiyuanyoujiao_subscribe,
        t7.subscribe    month_jiaoyuhuiyuanxiaoxue_subscribe,
        t8.subscribe   month_jiaoyuhuiyuanchuzhong_subscribe,
        t9.subscribe   month_jiaoyuhuiyuangaozhong_subscribe,
        t10.subscribe          month_yinyuehuiyuan_subscribe,
        t11.subscribe          month_kumiaohuiyuan_subscribe,
        t12.subscribe             month_yunshiting_subscribe,
        t13.subscribe            month_yinheshaoer_subscribe,
        t14.subscribe         month_shengjianyouxi_subscribe,
        t15.subscribe            month_migukuaiyou_subscribe,
        t16.subscribe     month_mengxiangyouxiting_subscribe,
        t17.subscribe          month_xuanjialeyuan_subscribe,
        t18.subscribe            month_leyoushijie_subscribe,
        t19.subscribe        month_dianjingfengbao_subscribe,
        t20.subscribe         month_touhaodianjing_subscribe,
        t21.subscribe     month_xingfujianshentuan_subscribe,
        t22.subscribe           month_jiuzhoulexue_subscribe,
        t23.subscribe                month_xueersi_subscribe,
        t24.subscribe             month_dierketang_subscribe,
        t25.subscribe    month_zhinengyuyinhuiyuan_subscribe,

        t1.unsubscribe              month_bailingkge_unsubscribe,
        t2.unsubscribe    month_chaojiyingshihuiyuan_unsubscribe,
        t3.unsubscribe      month_baiyingyoushenghuo_unsubscribe,
        t4.unsubscribe      month_chaojiyouxihuiyuan_unsubscribe,
        t5.unsubscribe   month_chaojidianjinghuiyuan_unsubscribe,
        t6.unsubscribe    month_jiaoyuhuiyuanyoujiao_unsubscribe,
        t7.unsubscribe    month_jiaoyuhuiyuanxiaoxue_unsubscribe,
        t8.unsubscribe   month_jiaoyuhuiyuanchuzhong_unsubscribe,
        t9.unsubscribe   month_jiaoyuhuiyuangaozhong_unsubscribe,
        t10.unsubscribe          month_yinyuehuiyuan_unsubscribe,
        t11.unsubscribe          month_kumiaohuiyuan_unsubscribe,
        t12.unsubscribe             month_yunshiting_unsubscribe,
        t13.unsubscribe            month_yinheshaoer_unsubscribe,
        t14.unsubscribe         month_shengjianyouxi_unsubscribe,
        t15.unsubscribe            month_migukuaiyou_unsubscribe,
        t16.unsubscribe     month_mengxiangyouxiting_unsubscribe,
        t17.unsubscribe          month_xuanjialeyuan_unsubscribe,
        t18.unsubscribe            month_leyoushijie_unsubscribe,
        t19.unsubscribe        month_dianjingfengbao_unsubscribe,
        t20.unsubscribe         month_touhaodianjing_unsubscribe,
        t21.unsubscribe     month_xingfujianshentuan_unsubscribe,
        t22.unsubscribe           month_jiuzhoulexue_unsubscribe,
        t23.unsubscribe                month_xueersi_unsubscribe,
        t24.unsubscribe             month_dierketang_unsubscribe,
        t25.unsubscribe    month_zhinengyuyinhuiyuan_unsubscribe
    FROM
    (
        SELECT usercode
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0)
        AND (feename like '%百灵K歌%' OR feename like '%超级影视会员%'OR feename like '%百映优生活%' OR feename like '%超级游戏会员%' OR feename like '%超级电竞会员%' OR feename like '%教育会员-幼教%' OR feename like '%教育会员-小学%' OR feename like '%教育会员-初中%' OR feename like '%教育会员-高中%' OR feename like '%音乐会员%' OR feename like '%酷喵会员%' OR feename like '%云视听%' OR feename like '%银河少儿%' OR feename like '%圣剑游戏%' OR feename like '%咪咕快游%' OR feename like '%梦想游戏厅%' OR feename like '%炫佳乐园%' OR feename like '%乐游世界%' OR feename like '%电竞风暴%' OR feename like '%头号电竞%' OR feename like '%幸福健身团%' OR feename like '%九州乐学%' OR feename like '%学而思%' OR feename like '%第二课堂%' OR feename like '%智能语音会员%' )
        GROUP BY usercode
    ) t0
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%百灵K歌%' 
        GROUP BY usercode, feename
    ) t1 ON t1.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%超级影视会员%'
        GROUP BY usercode, feename
    ) t2 ON t2.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%百映优生活%' 
        GROUP BY usercode, feename
    ) t3 ON t3.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%超级游戏会员%' 
        GROUP BY usercode, feename
    ) t4 ON t4.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%超级电竞会员%' 
        GROUP BY usercode, feename
    ) t5 ON t5.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%教育会员-幼教%' 
        GROUP BY usercode, feename
    ) t6 ON t6.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%教育会员-小学%' 
        GROUP BY usercode, feename
    ) t7 ON t7.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%教育会员-初中%' 
        GROUP BY usercode, feename
    ) t8 ON t8.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%教育会员-高中%' 
        GROUP BY usercode, feename
    ) t9 ON t9.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%音乐会员%' 
        GROUP BY usercode, feename
    ) t10 ON t10.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%酷喵会员%' 
        GROUP BY usercode, feename
    ) t11 ON t11.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%云视听%' 
        GROUP BY usercode, feename
    ) t12 ON t12.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%银河少儿%' 
        GROUP BY usercode, feename
    ) t13 ON t13.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%圣剑游戏%' 
        GROUP BY usercode, feename
    ) t14 ON t14.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%咪咕快游%' 
        GROUP BY usercode, feename
    ) t15 ON t15.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%梦想游戏厅%' 
        GROUP BY usercode, feename
    ) t16 ON t16.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%炫佳乐园%' 
        GROUP BY usercode, feename
    ) t17 ON t17.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%乐游世界%' 
        GROUP BY usercode, feename
    ) t18 ON t18.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%电竞风暴%' 
        GROUP BY usercode, feename
    ) t19 ON t19.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%头号电竞%' 
        GROUP BY usercode, feename
    ) t20 ON t20.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%幸福健身团%' 
        GROUP BY usercode, feename
    ) t21 ON t21.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%九州乐学%' 
        GROUP BY usercode, feename
    ) t22 ON t22.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%学而思%' 
        GROUP BY usercode, feename
    ) t23 ON t23.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%第二课堂%' 
        GROUP BY usercode, feename
    ) t24 ON t24.usercode = t0.usercode
    LEFT JOIN
    (
        SELECT 
            usercode, 
            MAX(CASE WHEN updatetype = 1 THEN 1 ELSE 0 END) subscribe,
            MAX(CASE WHEN updatetype = 0 THEN 1 ELSE 0 END) unsubscribe
        FROM knowyou_ott_ods.odm_mz_subs_di
        WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND updatetype IN (1,0) AND feename like '%智能语音会员%' 
        GROUP BY usercode, feename
    ) t25 ON t25.usercode = t0.usercode
) a
LEFT JOIN
(
    SELECT 
        deviceid,
        user_id,
        user_sub_id
    FROM knowyou_ott_ods.dim_cus_user_itv_df
    WHERE dt <='${C_DAY}' AND dt >= '${C_MONTH}01' AND length(deviceid) > 0 AND length(user_id) > 0 
) b ON b.user_id = a.usercode
;


-- 
-- 
--    █████▒█    ██  ▄████▄   ██ ▄█▀       ██████╗ ██╗   ██╗ ██████╗
--  ▓██   ▒ ██  ▓██▒▒██▀ ▀█   ██▄█▒        ██╔══██╗██║   ██║██╔════╝
--  ▒████ ░▓██  ▒██░▒▓█    ▄ ▓███▄░        ██████╔╝██║   ██║██║  ███╗
--  ░▓█▒  ░▓▓█  ░██░▒▓▓▄ ▄██▒▓██ █▄        ██╔══██╗██║   ██║██║   ██║
--  ░▒█░   ▒▒█████▓ ▒ ▓███▀ ░▒██▒ █▄       ██████╔╝╚██████╔╝╚██████╔╝
--   ▒ ░   ░▒▓▒ ▒ ▒ ░ ░▒ ▒  ░▒ ▒▒ ▓▒       ╚═════╝  ╚═════╝  ╚═════╝
--   ░     ░░▒░ ░ ░   ░  ▒   ░ ░▒ ▒░
--   ░ ░    ░░░ ░ ░ ░        ░ ░░ ░
--            ░     ░ ░      ░  ░
-- 
-- 1.13 行为标签-用户订购信息-ARPU值

INSERT OVERWRITE TABLE knowyou_ott_dmt.htv_arpu PARTITION(dt ='${C_DAY}')
SELECT  
    (CASE WHEN arpu >= 5  THEN 1 ELSE 0 END) AS arpu_five, -- 当日arpu是否大于等于5
    (CASE WHEN arpu >= 10 THEN 1 ELSE 0 END) AS arpu_ten, -- 当日arpu是否大于等于10
    (CASE WHEN arpu >= 20 THEN 1 ELSE 0 END) AS arpu_twenty, -- 当日arpu是否大于等于20
    (CASE WHEN arpu >= 30 THEN 1 ELSE 0 END) AS arpu_thirty -- 当日arpu是否大于等于30
FROM 
(
    SELECT sum(fee / 100) / count(*) AS arpu 
    FROM knowyou_ott_ods.odm_mz_subs_di
    WHERE updatetype = 1 AND dt = '${C_DAY}'
)a 
;