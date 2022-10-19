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
'20211219' as dt from knowyou_jituan_edw.edw_uba_cn_videoplayinfo 
where date_time>='20211205' and date_time<='20211219'
and playstatus='1' and videotype='点播'
group by deviceid,videoname,cp_name,contenttype,contentidfromepg;

---------------------------------------------------------------------------------------
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
'20220102' as dt from knowyou_jituan_edw.edw_uba_cn_videoplayinfo 
where date_time>='20211219' and date_time<='20220102'
and playstatus='1' and videotype='点播'
group by deviceid,videoname,cp_name,contenttype,contentidfromepg;

---------------------------------------------------------------------------------------
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
'20220103' as dt from knowyou_jituan_edw.edw_uba_cn_videoplayinfo 
where date_time>='20211220' and date_time<='20220103'
and playstatus='1' and videotype='点播'
group by deviceid,videoname,cp_name,contenttype,contentidfromepg;

---------------------------------------------------------------------------------------
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
'20220104' as dt from knowyou_jituan_edw.edw_uba_cn_videoplayinfo 
where date_time>='20211221' and date_time<='20220104'
and playstatus='1' and videotype='点播'
group by deviceid,videoname,cp_name,contenttype,contentidfromepg;

---------------------------------------------------------------------------------------
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
'20220105' as dt from knowyou_jituan_edw.edw_uba_cn_videoplayinfo 
where date_time>='20211222' and date_time<='20220105'
and playstatus='1' and videotype='点播'
group by deviceid,videoname,cp_name,contenttype,contentidfromepg;

---------------------------------------------------------------------------------------
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
'20220106' as dt from knowyou_jituan_edw.edw_uba_cn_videoplayinfo 
where date_time>='20211223' and date_time<='20220106'
and playstatus='1' and videotype='点播'
group by deviceid,videoname,cp_name,contenttype,contentidfromepg;

---------------------------------------------------------------------------------------
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
'20220107' as dt from knowyou_jituan_edw.edw_uba_cn_videoplayinfo 
where date_time>='20211224' and date_time<='20220107'
and playstatus='1' and videotype='点播'
group by deviceid,videoname,cp_name,contenttype,contentidfromepg;

---------------------------------------------------------------------------------------
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
'20220108' as dt from knowyou_jituan_edw.edw_uba_cn_videoplayinfo 
where date_time>='20211225' and date_time<='20220108'
and playstatus='1' and videotype='点播'
group by deviceid,videoname,cp_name,contenttype,contentidfromepg;

---------------------------------------------------------------------------------------
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
'20220109' as dt from knowyou_jituan_edw.edw_uba_cn_videoplayinfo 
where date_time>='20211226' and date_time<='20220109'
and playstatus='1' and videotype='点播'
group by deviceid,videoname,cp_name,contenttype,contentidfromepg;

---------------------------------------------------------------------------------------
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
'20220110' as dt from knowyou_jituan_edw.edw_uba_cn_videoplayinfo 
where date_time>='20211227' and date_time<='20220110'
and playstatus='1' and videotype='点播'
group by deviceid,videoname,cp_name,contenttype,contentidfromepg;

---------------------------------------------------------------------------------------
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
'20220111' as dt from knowyou_jituan_edw.edw_uba_cn_videoplayinfo 
where date_time>='20211228' and date_time<='20220111'
and playstatus='1' and videotype='点播'
group by deviceid,videoname,cp_name,contenttype,contentidfromepg;

---------------------------------------------------------------------------------------
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
'20220112' as dt from knowyou_jituan_edw.edw_uba_cn_videoplayinfo 
where date_time>='20211229' and date_time<='20220112'
and playstatus='1' and videotype='点播'
group by deviceid,videoname,cp_name,contenttype,contentidfromepg;

---------------------------------------------------------------------------------------
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
'20220113' as dt from knowyou_jituan_edw.edw_uba_cn_videoplayinfo 
where date_time>='20211230' and date_time<='20220113'
and playstatus='1' and videotype='点播'
group by deviceid,videoname,cp_name,contenttype,contentidfromepg;

---------------------------------------------------------------------------------------
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
'20220114' as dt from knowyou_jituan_edw.edw_uba_cn_videoplayinfo 
where date_time>='20211231' and date_time<='20220114'
and playstatus='1' and videotype='点播'
group by deviceid,videoname,cp_name,contenttype,contentidfromepg;

---------------------------------------------------------------------------------------
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
'20220115' as dt from knowyou_jituan_edw.edw_uba_cn_videoplayinfo 
where date_time>='20220101' and date_time<='20220115'
and playstatus='1' and videotype='点播'
group by deviceid,videoname,cp_name,contenttype,contentidfromepg;

---------------------------------------------------------------------------------------
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
'20220116' as dt from knowyou_jituan_edw.edw_uba_cn_videoplayinfo 
where date_time>='20220102' and date_time<='20220116'
and playstatus='1' and videotype='点播'
group by deviceid,videoname,cp_name,contenttype,contentidfromepg;

---------------------------------------------------------------------------------------
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
'20220117' as dt from knowyou_jituan_edw.edw_uba_cn_videoplayinfo 
where date_time>='20220103' and date_time<='20220117'
and playstatus='1' and videotype='点播'
group by deviceid,videoname,cp_name,contenttype,contentidfromepg;

