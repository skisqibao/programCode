select count(distinct video_id ) from ads_rec_userdetail_wtr  where dt=20210830;

select video_id, count(device_id ) from ads_rec_userdetail_wtr  
where dt = 20210830
GROUP BY video_id
HAVING count(device_id) > 1;


select count(distinct video_id ) from ads_rec_userdetail_wtr  where dt between 20210817 and 20210823;

select video_id, count(device_id ) from ads_rec_userdetail_wtr  
where dt between 20210824 and 20210830
GROUP BY video_id
HAVING count(device_id) > 1;

select video_id, count(device_id ) from ads_rec_userdetail_wtr  
where dt between 20210817 and 20210823
GROUP BY video_id
HAVING count(device_id) > 1;


SELECT video_id FROM ads_rec_userdetail_wtr  
WHERE dt = 20210830
GROUP BY video_id
HAVING count(device_id) > 1;




SELECT t1.device_id, t1.video_id FROM
(
SELECT device_id, video_id 
FROM knowyou_ott_dmt.ads_rec_userdetail_wtr 
WHERE dt BETWEEN 20211001 AND 20211031 
AND length(device_id) > 0 
AND length(video_id) > 0
GROUP BY device_id,video_id  
) t1
JOIN (
SELECT video_id, count(DISTINCT device_id) FROM knowyou_ott_dmt.ads_rec_userdetail_wtr  
WHERE dt BETWEEN 20211001 AND 20211031
GROUP BY video_id
HAVING count(DISTINCT device_id) > 1
) t2
ON t1.video_id = t2.video_id
JOIN (
SELECT device_id, count(DISTINCT video_id) FROM knowyou_ott_dmt.ads_rec_userdetail_wtr
WHERE dt BETWEEN 20211001 AND 20211031
GROUP BY device_id
HAVING count(DISTINCT video_id) > 5
) t3
ON t1.device_id = t3.device_id

SELECT t1.device_id, t1.video_id FROM ( SELECT device_id, video_id  FROM knowyou_ott_dmt.ads_rec_userdetail_wtr  WHERE dt BETWEEN 20210830 AND 20210831  AND length(device_id) > 0  AND length(video_id) > 0 GROUP BY device_id,video_id   ) t1 JOIN ( SELECT video_id FROM knowyou_ott_dmt.ads_rec_userdetail_wtr  WHERE dt BETWEEN 20210830 AND 20210831 GROUP BY video_id HAVING count(device_id) > 1 ) t2 ON t1.video_id = t2.video_id JOIN ( SELECT device_id FROM knowyou_ott_dmt.ads_rec_userdetail_wtr WHERE dt BETWEEN 20210830 AND 20210831 GROUP BY device_id HAVING count(video_id) > 5 ) t3 ON t1.device_id = t3.device_id

-- 加distinct
SELECT t1.device_id, t1.video_id FROM ( SELECT device_id, video_id FROM knowyou_ott_dmt.ads_rec_userdetail_wtr WHERE dt BETWEEN 20211001 AND 20211031 AND length(device_id) > 0  AND length(video_id) > 0 GROUP BY device_id, video_id) t1 JOIN ( SELECT video_id, COUNT(DISTINCT device_id) FROM knowyou_ott_dmt.ads_rec_userdetail_wtr WHERE dt BETWEEN 20211001 AND 20211031 GROUP BY video_id HAVING COUNT(DISTINCT device_id) > 1 ) t2 ON t1.video_id = t2.video_id JOIN ( SELECT device_id, COUNT(DISTINCT video_id) FROM knowyou_ott_dmt.ads_rec_userdetail_wtr WHERE dt BETWEEN 20211001 AND 20211031 GROUP BY device_id HAVING COUNT(DISTINCT video_id) > 5 ) t3 ON t1.device_id = t3.device_id


-- 产品数       135
SELECT COUNT(distinct productionname) FROM knowyou_ott_dmt.production_behavior 
WHERE datetime BETWEEN 20211101 AND 20211130
AND length(productionname) > 0

-- 订购用户数   95323
SELECT COUNT(distinct deviceid) FROM knowyou_ott_dmt.production_behavior 
WHERE datetime BETWEEN 20211101 AND 20211130
AND length(deviceid) > 0


-- 订购用户观看数量平均值
-- 4339014 / 59745 = 72.62555862415265
SELECT SUM(cnt)/COUNT(device_id) FROM
(    
    SELECT t1.device_id AS device_id, COUNT(DISTINCT t1.video_id) AS cnt FROM
    (
        SELECT device_id, video_id 
        FROM knowyou_ott_dmt.ads_rec_userdetail_wtr 
        WHERE dt BETWEEN 20211001 AND 20211031 AND length(device_id) > 0
    ) t1 
    JOIN
    (
        SELECT deviceid FROM knowyou_ott_dmt.production_behavior 
        WHERE datetime BETWEEN 20211001 AND 20211031 AND length(deviceid) > 0
        GROUP BY deviceid
    ) t2
    ON t1.device_id = t2.deviceid
    GROUP BY t1.device_id
) t3

-- 未订购用户观看数量平均值
-- 65167789 / 1343516 = 48.505405964647984
SELECT SUM(cnt)/COUNT(device_id) FROM
(    
    SELECT t1.device_id AS device_id, COUNT(DISTINCT t1.video_id) AS cnt FROM
    (
        SELECT device_id, video_id 
        FROM knowyou_ott_dmt.ads_rec_userdetail_wtr 
        WHERE dt BETWEEN 20211001 AND 20211031 AND length(device_id) > 0 
    ) t1 
    LEFT JOIN
    (
        SELECT deviceid FROM knowyou_ott_dmt.production_behavior 
        WHERE datetime BETWEEN 20211001 AND 20211031 AND length(deviceid) > 0
        GROUP BY deviceid
    ) t2
    ON t1.device_id = t2.deviceid
    WHERE t2.deviceid IS NULL
    GROUP BY t1.device_id
) t3

-- 未订购用户取样观看数量平均值
SELECT SUM(cnt), COUNT(device_id), SUM(cnt)/COUNT(device_id) FROM
(   
    SELECT * FROM
    ( 
        SELECT t1.device_id AS device_id, COUNT(DISTINCT t1.video_id) AS cnt FROM
        (
            SELECT device_id, video_id 
            FROM knowyou_ott_dmt.ads_rec_userdetail_wtr 
            WHERE dt BETWEEN 20211001 AND 20211031 AND length(device_id) > 0
            GROUP BY device_id, video_id
        ) t1 
        LEFT JOIN
        (
            SELECT deviceid FROM knowyou_ott_dmt.production_behavior 
            WHERE datetime BETWEEN 20211001 AND 20211031 AND length(deviceid) > 0
            GROUP BY deviceid
        ) t2
        ON t1.device_id = t2.deviceid
        WHERE t2.deviceid IS NULL
        GROUP BY t1.device_id
    ) t3
    DISTRIBUTE BY rand() SORT BY rand() LIMIT 59745
) t4



-- 订购观看量   可用******
SELECT SUM(cnt), COUNT(device_id), SUM(cnt)/COUNT(device_id) FROM
(   
    SELECT device_id, COUNT(DISTINCT video_id) AS cnt FROM
    (
        SELECT t1.device_id, t1.video_id  FROM
        (
            SELECT device_id, video_id 
            FROM knowyou_ott_dmt.ads_rec_userdetail_wtr 
            WHERE dt BETWEEN 20220201 AND 20220228 
            AND length(device_id) > 0 
            AND length(video_id) > 0
            GROUP BY device_id, video_id 
        ) t1 
        JOIN
        (
            SELECT deviceid FROM knowyou_ott_dmt.production_behavior 
            WHERE datetime BETWEEN 20220201 AND 20220228 AND length(deviceid) > 0
            GROUP BY deviceid
        ) t4
        ON t1.device_id = t4.deviceid
    ) tt
    GROUP BY device_id
) t

-- 未订购观看量   可用******
SELECT SUM(cnt), COUNT(device_id), SUM(cnt)/COUNT(device_id) FROM
(   
    SELECT device_id, COUNT(DISTINCT video_id) AS cnt FROM
    (
        SELECT t1.device_id, t1.video_id  FROM
        (
            SELECT device_id, video_id 
            FROM knowyou_ott_dmt.ads_rec_userdetail_wtr 
            WHERE dt BETWEEN 20220201 AND 20220228 
            AND length(device_id) > 0 
            AND length(video_id) > 0
            GROUP BY device_id, video_id 
        ) t1 
        LEFT JOIN
        (
            SELECT deviceid FROM knowyou_ott_dmt.production_behavior 
            WHERE datetime BETWEEN 20220201 AND 20220228 AND length(deviceid) > 0
            GROUP BY deviceid
        ) t4
        ON t1.device_id = t4.deviceid
        WHERE t4.deviceid IS NULL
    ) tt
    GROUP BY device_id
) t

-- 筛选数据集   可用******
SELECT SUM(cnt), COUNT(device_id), SUM(cnt)/COUNT(device_id) FROM
(   
    SELECT device_id, COUNT(DISTINCT video_id) AS cnt FROM
    (
        SELECT t1.device_id, t1.video_id  FROM
        (
            SELECT device_id, video_id 
            FROM knowyou_ott_dmt.ads_rec_userdetail_wtr 
            WHERE dt BETWEEN 20220201 AND 20220228 
            AND length(device_id) > 0 
            AND length(video_id) > 0
            GROUP BY device_id, video_id 
        ) t1 
        JOIN (
            SELECT video_id FROM knowyou_ott_dmt.ads_rec_userdetail_wtr  
            WHERE dt BETWEEN 20220201 AND 20220228
            GROUP BY video_id
            HAVING count(device_id) > 1
        ) t2
        ON t1.video_id = t2.video_id
        JOIN (
            SELECT device_id FROM knowyou_ott_dmt.ads_rec_userdetail_wtr
            WHERE dt BETWEEN 20220201 AND 20220228
            GROUP BY device_id
            HAVING count(video_id) > 200     
        ) t3
        ON t1.device_id = t3.device_id
        LEFT JOIN
        (
            SELECT deviceid FROM knowyou_ott_dmt.production_behavior 
            WHERE datetime BETWEEN 20220201 AND 20220228 AND length(deviceid) > 0
            GROUP BY deviceid
        ) t4
        ON t1.device_id = t4.deviceid
        WHERE t4.deviceid IS NULL
    ) tt
    GROUP BY device_id
) t



1.针对1元影视包做所有高活跃用户的评分预测。
2.按照评分从大到小排序，截取前50000个。
3.输出这50000个用户的信息到excel文件中，字段包括
用户id（device_id），推荐产品包名（影视月包），推荐度（评分），
历史订购量（去重订购数），历史订购包名（中文产品包名集合），
历史观看量（去重节目数），历史观看节目名（中文节目名集合）

SELECT
    s4.userid,
    s1.device_id,
    s1.prod_id,
    s1.ratingf,
    s1.rn,
    s2.p_cnt,
    s2.p_list,
    s3.v_cnt,
    s3.v_list
FROM
(
    SELECT device_id, prod_id, ratingf, rn FROM
    (
        SELECT 
            device_id, 
            prod_id,
            ratingf,
            ROW_NUMBER() OVER(PARTITION BY 0 ORDER BY ratingf DESC) AS rn
        FROM 
        (
            SELECT 
                device_id, prod_id, 
                CAST(CAST(rating AS double) AS DECIMAL(18, 10)) AS ratingf
            FROM knowyou_ott_dmt.htv_pay_plus_temp
            WHERE week = '1ypackageresult8'
        ) t
    ) t
    WHERE rn <= 12000
) s1
LEFT JOIN
(
    SELECT deviceid, COUNT(productionname) p_cnt, concat_ws(',', collect_set(productionname)) p_list
    FROM
    (
        SELECT deviceid, productionname 
        FROM knowyou_ott_dmt.production_behavior 
        WHERE datetime BETWEEN 20211001 AND 20211031 AND length(deviceid) > 0
        GROUP BY deviceid, productionname
    ) t
    GROUP BY deviceid
) s2
ON s1.device_id = s2.deviceid
LEFT JOIN
(
    SELECT device_id, COUNT(video_id) v_cnt, concat_ws(',', collect_set(video_name)) v_list
    FROM
    (
        SELECT device_id, video_id, video_name 
        FROM knowyou_ott_dmt.ads_rec_userdetail_wtr 
        WHERE dt BETWEEN 20211001 AND 20211031 AND length(device_id) > 0 AND length(video_id) > 0
        GROUP BY device_id, video_id, video_name
    ) t
    GROUP BY device_id
) s3
ON s1.device_id = s3.device_i
LEFT JOIN
(
    SELECT deviceid, userid FROM knowyou_ott_dmt.deviceid_userid WHERE day_id = 20211209
) s4
ON s1.device_id = s4.deviceid
JOIN
(
    SELECT deviceid FROM knowyou_ott_dmt.watch_vip_user GROUP BY deviceid 
) s5 
ON s1.device_id = s5.deviceid
ORDER BY s1.rn


-- 
SELECT ord10.deviceid, ord11.deviceid, ord10.productionname, ord10.v_list, ord11.v_list
FROM
(
    SELECT deviceid, productionname, v_list
    FROM
    (
        SELECT deviceid, productionname 
        FROM knowyou_ott_dmt.production_behavior 
        WHERE datetime BETWEEN 20211001 AND 20211031 AND length(deviceid) > 0
        AND productionname = '爱家影视月包（首月优惠）'
        GROUP BY deviceid, productionname
    ) s2
    LEFT JOIN
    (
        SELECT device_id, concat_ws(',', collect_set(video_name)) v_list
        FROM
        (
            SELECT device_id, video_name 
            FROM knowyou_ott_dmt.ads_rec_userdetail_wtr 
            WHERE dt BETWEEN 20211001 AND 20211031 AND length(device_id) > 0 AND length(video_id) > 0
            GROUP BY device_id, video_name
        ) t
        GROUP BY device_id
    ) s3
    ON s2.deviceid = s3.device_id
) ord10
FULL JOIN 
(
    SELECT deviceid, productionname, v_list
    FROM
    (
        SELECT deviceid, productionname 
        FROM knowyou_ott_dmt.production_behavior 
        WHERE datetime BETWEEN 20211101 AND 20211130 AND length(deviceid) > 0
        AND productionname = '爱家影视月包（首月优惠）'
        GROUP BY deviceid, productionname
    ) s2
    LEFT JOIN
    (
        SELECT device_id, concat_ws(',', collect_set(video_name)) v_list
        FROM
        (
            SELECT device_id, video_name 
            FROM knowyou_ott_dmt.ads_rec_userdetail_wtr 
            WHERE dt BETWEEN 20211101 AND 20211130 AND length(device_id) > 0 AND length(video_id) > 0
            GROUP BY device_id, video_name
        ) t
        GROUP BY device_id
    ) s3
    ON s2.deviceid = s3.device_id
) ord11
ON ord10.deviceid = ord11.deviceid



-- 观看节目(去重）次数与订购率关系
SELECT 
    tot.video_cnt
    , total_cnt
    , NVL(order_cnt, 0)
    , regexp_extract(NVL(order_cnt/total_cnt, 0.00),'([0-9]*.[0-9][0-9])' ,1) AS ratio 
FROM
(
    SELECT video_cnt, COUNT(device_id) AS total_cnt FROM
    (
        SELECT device_id, COUNT(DISTINCT video_id) AS video_cnt
        FROM knowyou_ott_dmt.ads_rec_userdetail_wtr 
        WHERE dt BETWEEN 20211001 AND 20211031 AND length(device_id) > 0
        GROUP BY device_id
    ) t1
    GROUP BY video_cnt 
) tot
LEFT JOIN
(
    SELECT video_cnt, COUNT(device_id) AS order_cnt FROM
    (
        SELECT device_id, video_cnt FROM
        (
            SELECT device_id, COUNT(DISTINCT video_id) AS video_cnt
            FROM knowyou_ott_dmt.ads_rec_userdetail_wtr 
            WHERE dt BETWEEN 20211001 AND 20211031 AND length(device_id) > 0
            GROUP BY device_id
        ) t1
        JOIN
        (
            SELECT deviceid FROM knowyou_ott_dmt.production_behavior 
            WHERE datetime BETWEEN 20211001 AND 20211031 AND length(deviceid) > 0
            GROUP BY deviceid
        ) t2
        ON t1.device_id = t2.deviceid
    ) t3
    GROUP BY video_cnt 
) ord
ON tot.video_cnt = ord.video_cnt
ORDER BY tot.video_cnt 
;

---------------------------------------------------
SELECT
    base64(CAST(deviceid AS binary)) deviceid,
    actualtime,
    playtime,
    seriesheadcode,
    series_name,
    series_searchname,
    series_status,
    series_description,
    series_type,
    series_cmsid,
    series_isfree,
    series_director,
    series_actordisplay,
    series_originalcountry,
    series_language,
    series_keywords,
    package_series,
    picture_series_1_1_fileurl,
    dt
FROM knowyou_ott_ods.dws_rec_video_playinfo_di
WHERE dt IN (SELECT MAX(dt) FROM knowyou_ott_ods.dws_rec_video_playinfo_di) 
;


SELECT
    base64(CAST(deviceid AS binary)) deviceid,
    actualtime,
    cpname,
    searchcontent,
    dt
FROM knowyou_ott_ods.dwd_fct_log_click_search_di
WHERE dt IN (SELECT MAX(dt) FROM knowyou_ott_ods.dwd_fct_log_click_search_di)
;


SELECT
    series,
    series_name,
    series_originalname, 
    series_orgairdate, 
    series_licensingwindowstart, 
    series_licensingwindowend, 
    series_displayasnew, 
    series_displayaslastchance, 
    series_macrovision, 
    series_price, 
    series_volumncount, 
    series_status, 
    series_description, 
    series_type, 
    series_titlesc, 
    series_score, 
    series_imagesc, 
    series_updatetimesc, 
    series_cmsid, 
    series_isfree, 
    series_director, 
    series_actordisplay, 
    series_originalcountry, 
    series_language, 
    series_keywords, 
    package_series, 
    createtime, 
    picture_series_1_1_fileurl, 
    picture_series_1_1_height, 
    picture_series_1_1_rate, 
    picture_series_1_1_width,
    dt
FROM knowyou_ott_ods.dim_pub_video_df
WHERE dt IN (SELECT MAX(dt) FROM knowyou_ott_ods.dim_pub_video_df) AND series_status = '0'
;


SELECT
    base64(CAST(broadband_id AS binary)) broadband_id,
    base64(CAST(user_id AS binary)) user_id,
    base64(CAST(user_sub_id AS binary)) user_sub_id,
    city_id,
    phone_no,
    prod_id,
    prod_name,
    pkg_id,
    pkg_name,
    create_org_id,
    ope_type,
    ord_time,
    eff_time,
    exp_time,
    fee,
    mac,
    sp_id,
    dt
FROM knowyou_ott_ods.dwd_jf_prod_ord_di
WHERE dt IN (SELECT MAX(dt) FROM knowyou_ott_ods.dwd_jf_prod_ord_di)
;



SELECT
    base64(CAST(broadband_id As binary)) broadband_id, 
    base64(CAST(user_id As binary)) user_id, 
    base64(CAST(user_sub_id As binary)) user_sub_id, 
    base64(CAST(device_id As binary)) device_id, 
    city_id, 
    broadband_type, 
    is_school, 
    broadband_status, 
    broadband_ord_time, 
    broadband_eff_time, 
    broadband_exp_time, 
    htv_status, 
    ord_time, 
    eff_time, 
    exp_time, 
    active_status, 
    active_time,  
    mac, 
    sp_id,
    dt
FROM knowyou_ott_ods.dwd_jf_user_itv_df
WHERE dt IN (SELECT MAX(dt) FROM knowyou_ott_ods.dwd_jf_user_itv_df)
;

SELECT 
    prod_id,
    prod_name,
    pkg_id,
    pkg_name,
    fee,
    dt
FROM knowyou_ott_ods.ods_jf_prod_info
WHERE dt IN (SELECT MAX(dt) FROM knowyou_ott_ods.ods_jf_prod_info)
;

hive -f dim_pub_video_df.sql > sync_data/${cur_date}/dim_pub_video_df.txt
hive -f dwd_fct_log_click_search_di.sql > sync_data/${cur_date}/dwd_fct_log_click_search_di.txt
hive -f dwd_jf_prod_ord_di.sql > sync_data/${cur_date}/dwd_jf_prod_ord_di.txt
hive -f dwd_jf_user_itv_df.sql > sync_data/${cur_date}/dwd_jf_user_itv_df.txt
hive -f dws_rec_video_playinfo_di.sql > sync_data/${cur_date}/dws_rec_video_playinfo_di.txt
hive -f ods_jf_prod_info.sql > sync_data/${cur_date}/ods_jf_prod_info.txt

sftp -P22222 cmccah@183.204.51.186
PJscaSqcI0M=

lftp -u cmccah,PJscaSqcI0M= sftp://183.204.51.186:22222


select dt,count(distinct deviceid) from knowyou_ott_ods.dim_mbh_user_itv_df 
where dt>=20220801 and bootstatusday='1' group by dt order by dt;
    20220801    1785557     ||    1785565    UNION    599025    =>    1887967
    20220802    1805021     ||    1805029    UNION    588026    =>    1905693
    20220803    1791552     ||    1791560    UNION    577237    =>    1891437
    20220804    1697734     ||    1697739    UNION    572111    =>    1808383
    20220805    1768271     ||    1768278    UNION    580014    =>    1872068
    20220806    1765058     ||    1765064    UNION    598223    =>    1871602
    20220807    1763914     ||    1763918    UNION    614241    =>    1872906
    20220808    1711901     ||    1711908    UNION    593960    =>    1823056
    20220809    1689601     ||    1689609    UNION    582918    =>    1799341
    20220810    1712928     ||    1712936    UNION    580249    =>    1818259
    20220811    1424810     ||    1424818    UNION    581329    =>    1577965
    20220812    1745209     ||    1745214    UNION    600948    =>    1852296
    20220813    1504867     ||    1504873    UNION    618938    =>    1659196
    20220814    1335173     ||    1335179    UNION    612801    =>    1522741
    20220815    1321554     ||    1321558    UNION    595956    =>    1499113

select dt,count(distinct deviceid) from knowyou_ott_ods.odm_jituan_info_tmp 
where dt>=20220801 group by dt order by dt;

select dt,count(distinct deviceid) from knowyou_ott_ods.odm_cp_play_di 
where dt>=20220801 group by dt order by dt;

select dt,count(distinct deviceid) from
(
    select dt,deviceid from knowyou_ott_ods.odm_jituan_info_tmp 
    where dt>=20220801 and length(deviceid) > 2 group by dt,deviceid
    UNION 
    select dt,deviceid from knowyou_ott_ods.odm_cp_play_di 
    where dt>=20220801 and length(deviceid) > 2 group by dt,deviceid
) a
group by dt order by dt;



select count(*) from 
(
SELECT deviceid, t2.user_sub_id, NVL(t2.mac, concat_ws(':',substr(t1.mac,-12,2),substr(t1.mac,-10,2),substr(t1.mac,-8,2),substr(t1.mac,-6,2),substr(t1.mac,-4,2),substr(t1.mac,-2,2))), t2.sp_id , latest_time FROM
(
    select deviceid, mac, max(latest_time) latest_time from
    (
        select deviceid, substr(deviceid, -12, 12) mac, max(from_unixtime( cast(
            if(
                from_unixtime(cast(substr(get_json_object(get_json_object(json_data,'$.actionInfo'),'$.programId'),1,10) as bigint),'yyyyMMdd') = dt,
                substr(get_json_object(get_json_object(json_data,'$.actionInfo'),'$.programId'),1,10),
                if(
                    from_unixtime(cast(substr(get_json_object(json_data,'$.timestamp'),1,10) as bigint),'yyyyMMdd') = dt,
                    substr(get_json_object(json_data,'$.timestamp'),1,10),
                    substr(actualtime,1,10) 
                )  
            ) as int) )) as latest_time
        from knowyou_ott_ods.odm_jituan_info_tmp 
        where dt = 20220801 and length(deviceid) = 32 group by deviceid
        UNION 
        select deviceid, substr(deviceid, -12, 12) mac, max(from_unixtime(unix_timestamp(substr(data_time,1,14), 'yyyyMMddHHmmss'), 'yyyy-MM-dd HH:mm:ss')) latest_time
        from knowyou_ott_ods.odm_cp_logininit_di 
        where dt = 20220801 and length(deviceid) = 32 group by deviceid
    ) t
    group by deviceid, mac
) t1
LEFT JOIN 
(
    SELECT user_sub_id, mac, case when sp_id = '02' then '未来电视' when sp_id = '03' then '百视通' else '其他' end sp_id FROM
    (
        SELECT
            user_sub_id, mac, sp_id, row_number() over(partition by mac order by active_time desc) rn
        FROM knowyou_ott_ods.dwd_jf_user_itv_df
        WHERE dt = 20220812 and length(mac) = 17 and length(active_time) > 0 
    )t  WHERE rn = 1
) t2
ON t1.mac = REGEXP_REPLACE(t2.mac, ':', '')

where t2.mac is null
) x
;

270010    user_sub_id is NULL    

select sp_id, count(*) from
(
SELECT 
    deviceid, 
    t2.user_sub_id user_sub_id, 
    NVL(t2.mac, concat_ws(':',substr(t1.mac,-12,2),substr(t1.mac,-10,2),substr(t1.mac,-8,2),substr(t1.mac,-6,2),substr(t1.mac,-4,2),substr(t1.mac,-2,2))) mac, 
    case when t2.sp_id = '02' then '未来电视' when t2.sp_id = '03' then '百视通' else '其他' end sp_id, 
    latest_time 
FROM
(
    select deviceid, mac, max(latest_time) latest_time from
    (
        select deviceid, substr(deviceid, -12, 12) mac, max(from_unixtime( cast(
            if(
                from_unixtime(cast(substr(get_json_object(get_json_object(json_data,'$.actionInfo'),'$.programId'),1,10) as bigint),'yyyyMMdd') = dt,
                substr(get_json_object(get_json_object(json_data,'$.actionInfo'),'$.programId'),1,10),
                if(
                    from_unixtime(cast(substr(get_json_object(json_data,'$.timestamp'),1,10) as bigint),'yyyyMMdd') = dt,
                    substr(get_json_object(json_data,'$.timestamp'),1,10),
                    substr(actualtime,1,10) 
                )  
            ) as int) )) as latest_time
        from knowyou_ott_ods.odm_jituan_info_tmp 
        where dt = 20220801 and length(deviceid) = 32 group by deviceid
        UNION 
        select deviceid, substr(deviceid, -12, 12) mac, max(from_unixtime(unix_timestamp(substr(data_time,1,14), 'yyyyMMddHHmmss'), 'yyyy-MM-dd HH:mm:ss')) latest_time
        from knowyou_ott_ods.odm_cp_logininit_di 
        where dt = 20220801 and length(deviceid) = 32 group by deviceid
    ) t
    group by deviceid, mac
) t1
LEFT JOIN 
(
    SELECT user_sub_id, mac, sp_id FROM
    (
        SELECT
            user_sub_id, mac, sp_id, row_number() over(partition by mac order by active_time desc) rn
        FROM knowyou_ott_ods.dwd_jf_user_itv_df
        WHERE dt = 20220812 and length(mac) = 17 and length(active_time) > 0 
    )t  WHERE rn = 1
) t2
ON t1.mac = REGEXP_REPLACE(t2.mac, ':', '')
) x 
group by sp_id

select licences, count(*) from
select count(*) from
(

select 
    aa.deviceid,
    nvl(bb.zi_id, aa.user_sub_id)as user_sub_id,
    aa.mac,
    nvl(bb.licences, aa.sp_id) as licences,
    aa.latest_time
from 
(SELECT 
    deviceid, 
    t2.user_sub_id, 
    NVL(t2.mac, concat_ws(':',substr(t1.mac,-12,2),substr(t1.mac,-10,2),substr(t1.mac,-8,2),substr(t1.mac,-6,2),substr(t1.mac,-4,2),substr(t1.mac,-2,2)))as mac, 
    case when t2.sp_id = '02' then '未来电视' when t2.sp_id = '03' then '百视通' else '其他' end sp_id, 
    latest_time 
FROM
(
    select deviceid, mac, max(latest_time) latest_time from
    (
        select deviceid, substr(deviceid, -12, 12) mac, max(from_unixtime( cast(
            if(
                from_unixtime(cast(substr(get_json_object(get_json_object(json_data,'$.actionInfo'),'$.programId'),1,10) as bigint),'yyyyMMdd') = dt,
                substr(get_json_object(get_json_object(json_data,'$.actionInfo'),'$.programId'),1,10),
                if(
                    from_unixtime(cast(substr(get_json_object(json_data,'$.timestamp'),1,10) as bigint),'yyyyMMdd') = dt,
                    substr(get_json_object(json_data,'$.timestamp'),1,10),
                    substr(actualtime,1,10) 
                )  
            ) as int) )) as latest_time
        from knowyou_ott_ods.odm_jituan_info_tmp 
        where dt = 20220801 and length(deviceid) = 32 group by deviceid
        UNION 
        select deviceid, substr(deviceid, -12, 12) mac, max(from_unixtime(unix_timestamp(substr(data_time,1,14), 'yyyyMMddHHmmss'), 'yyyy-MM-dd HH:mm:ss')) latest_time
        from knowyou_ott_ods.odm_cp_logininit_di 
        where dt = 20220801 and length(deviceid) = 32 group by deviceid
    ) t
    group by deviceid, mac
) t1
LEFT JOIN 
(
    SELECT user_sub_id, mac, sp_id FROM
    (
        SELECT
            user_sub_id, mac, sp_id, row_number() over(partition by mac order by active_time desc) rn
        FROM knowyou_ott_ods.dwd_jf_user_itv_df
        WHERE dt = 20220812 and length(mac) = 17 and length(active_time) > 0 
    )t  WHERE rn = 1
) t2
ON t1.mac = REGEXP_REPLACE(t2.mac, ':', ''))aa 
left join 
(select  licences,zi_id,mac from default.rec_user_list_dd )bb
on substr(aa.deviceid, -12, 12) =bb.mac
) x 
where user_sub_id is null
group by licences

select deviceid, mac, latest_time FROM
(
    select deviceid, mac, max(latest_time) latest_time from
    (
        select deviceid, substr(deviceid, -12, 12) mac, max(from_unixtime( cast(
            if(
                from_unixtime(cast(substr(get_json_object(get_json_object(json_data,'$.actionInfo'),'$.programId'),1,10) as bigint),'yyyyMMdd') = dt,
                substr(get_json_object(get_json_object(json_data,'$.actionInfo'),'$.programId'),1,10),
                if(
                    from_unixtime(cast(substr(get_json_object(json_data,'$.timestamp'),1,10) as bigint),'yyyyMMdd') = dt,
                    substr(get_json_object(json_data,'$.timestamp'),1,10),
                    if(
                        from_unixtime(cast(substr(actualtime,1,10) as bigint),'yyyyMMdd') = dt,
                        substr(actualtime,1,10),
                        NULL   
                    )                  
                )  
            ) as int) )) as latest_time
        from knowyou_ott_ods.odm_jituan_info_tmp 
        where dt = 20220801 and length(deviceid) = 32 group by deviceid
        UNION 
        select deviceid, substr(deviceid, -12, 12) mac, max(from_unixtime(unix_timestamp(substr(data_time,1,14), 'yyyyMMddHHmmss'), 'yyyy-MM-dd HH:mm:ss')) latest_time
        from knowyou_ott_ods.odm_cp_logininit_di 
        where dt = 20220801 and length(deviceid) = 32 group by deviceid
    ) t
    group by deviceid, mac 
    order by    latest_time desc  
)x
group by latest_time
order by latest_time desc 

arr+=(20220801)
arr+=(20220802)
arr+=(20220803)
arr+=(20220804)
arr+=(20220805)
arr+=(20220806)
arr+=(20220807)
arr+=(20220808)
arr+=(20220809)
arr+=(20220810)
arr+=(20220811)
arr+=(20220812)
arr+=(20220813)
arr+=(20220814)
arr+=(20220815)


for datedate in ${arr[@]}
do
    hive -e "select 
                aa.deviceid,
                nvl(bb.zi_id, aa.user_sub_id)as user_sub_id,
                aa.mac,
                nvl(bb.licences, aa.sp_id) as licences,
                aa.latest_time
            from 
            (SELECT 
                deviceid, 
                t2.user_sub_id, 
                NVL(t2.mac, concat_ws(':',substr(t1.mac,-12,2),substr(t1.mac,-10,2),substr(t1.mac,-8,2),substr(t1.mac,-6,2),substr(t1.mac,-4,2),substr(t1.mac,-2,2)))as mac, 
                case when t2.sp_id = '02' then 'YST' when t2.sp_id = '03' then 'BESTV' else 'NULL' end sp_id, 
                latest_time 
            FROM
            (
                select deviceid, mac, max(latest_time) latest_time from
                (
                    select deviceid, substr(deviceid, -12, 12) mac, max(from_unixtime( cast(
                        if(
                            from_unixtime(cast(substr(get_json_object(get_json_object(json_data,'$.actionInfo'),'$.programId'),1,10) as bigint),'yyyyMMdd') = dt,
                            substr(get_json_object(get_json_object(json_data,'$.actionInfo'),'$.programId'),1,10),
                            if(
                                from_unixtime(cast(substr(get_json_object(json_data,'$.timestamp'),1,10) as bigint),'yyyyMMdd') = dt,
                                substr(get_json_object(json_data,'$.timestamp'),1,10),
                                substr(actualtime,1,10) 
                            )  
                        ) as int) )) as latest_time
                    from knowyou_ott_ods.odm_jituan_info_tmp 
                    where dt = 20220815 and length(deviceid) = 32 group by deviceid
                    UNION                     
                    select deviceid, substr(deviceid, -12, 12) mac, max(from_unixtime(unix_timestamp(substr(data_time,1,14), 'yyyyMMddHHmmss'), 'yyyy-MM-dd HH:mm:ss')) latest_time
                    from knowyou_ott_ods.odm_cp_logininit_di 
                    where dt = 20220815 and length(deviceid) = 32 group by deviceid
                ) t
                group by deviceid, mac
            ) t1
            LEFT JOIN 
            (
                SELECT user_sub_id, mac, sp_id FROM
                (
                    SELECT
                        user_sub_id, mac, sp_id, row_number() over(partition by mac order by active_time desc) rn
                    FROM knowyou_ott_ods.dwd_jf_user_itv_df
                    WHERE dt = 20220812 and length(mac) = 17 and length(active_time) > 0 
                )t  WHERE rn = 1
            ) t2
            ON t1.mac = REGEXP_REPLACE(t2.mac, ':', ''))aa 
            left join 
            (select case when licences = '未来电视' then 'YST' when licences = '百视通' then 'BESTV' else 'NULL' end  licences,zi_id,mac from default.rec_user_list_dd )bb
            on substr(aa.deviceid, -12, 12) =bb.mac" | sed 's/\t/,/g' > data2/data_20220815.txt

done
