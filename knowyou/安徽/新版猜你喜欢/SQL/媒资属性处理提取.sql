INSERT OVERWRITE TABLE knowyou_ott_dmt.guess_like_media_attribute PARTITION (dt = '${C_DAY}')
SELECT 
  t1.series AS video_id,
  t1.series_name AS video_name, 
  t1.year AS year,
  t2.series_isfree AS is_free, 
  t3.video_type AS content_type, 
  t4.director AS director, 
  t5.actor AS actor, 
  t6.regin AS regin, 
  t7.language AS language,
  t8.plot AS plot
FROM 
( 
  SELECT
    series,
    series_name,
    CASE WHEN year IN ('', 'NULL') THEN '未知'
    WHEN year IS NULL THEN '未知'
    WHEN year IN ('2010','2011','2012','2013','2014','2015','2016','2017','2018','2019','2020','2021','2022') THEN year
    ELSE '2009及以前' END year
  FROM
  (
    SELECT
      series,
      series_name,
      substr(series_orgairdate, 0, 4) AS year
    FROM knowyou_ott_ods.dim_pub_video_df
    WHERE dt = '${C_DAY}' 
    GROUP BY series, series_name, substr(series_orgairdate, 0, 4)
  ) a 
) t1
JOIN 
(
  SELECT
    series, series_isfree
  FROM knowyou_ott_ods.dim_pub_video_df
  WHERE dt = '${C_DAY}' AND series_isfree IN ('0', '1')
) t2 
ON t1.series = t2.series
JOIN
(
  SELECT series, video_type FROM
  (
    SELECT
      series,
      (
        CASE WHEN series_type IN ('电视台') THEN '电视台'
        WHEN series_type IN ('4K', '电影') THEN '电影'
        WHEN series_type IN ('连续剧', '电视剧') THEN '电视剧'
        WHEN series_type IN ('0', '熊猫', '少儿') THEN '少儿'
        WHEN series_type IN ('旅游', '综艺') THEN '综艺'
        WHEN series_type IN ('音乐') THEN '音乐'
        WHEN series_type IN ('运动', '体育') THEN '体育'
        WHEN series_type IN ('法治', '知识', '教育') THEN '教育'
        WHEN series_type IN ('片花') THEN '片花'
        WHEN series_type IN ('纪录片') THEN '纪录片'
        WHEN series_type IN ('娱乐') THEN '娱乐'
        WHEN series_type IN ('时尚') THEN '时尚'
        WHEN series_type IN ('搞笑') THEN '搞笑'
        WHEN series_type IN ('游戏') THEN '游戏'
        WHEN series_type IN ('财经', '资讯', '新闻') THEN '新闻'
        WHEN series_type IN ('高清', 'DIY', '健身', '养生', '动作', '戏曲', '时尚生活', '瑜伽', '职业', '艺术', '健康', '太极', '孕育', '家居', '曲艺', '武术', '母婴', '美颜', '美食', '舞蹈', '生活') THEN '生活'
        WHEN series_type IN ('动漫') THEN '动漫'
        WHEN series_type IN ('栏目', '汽车2', '休闲', '央视名栏', '文化', '科技', '短视频', '其他') THEN '其他'
        WHEN series_type IN ('itvyc002', 'itvyc003', 'itvyc004', 'itvyc005') THEN '测试'
        WHEN series_type IN ('政论片', '党建影视', '党建时政', '党建资讯') THEN '党建'
        WHEN series_type IN ('电子竞技', '电竞') THEN '电竞'
        WHEN series_type IN ('购物') THEN '购物'
        WHEN series_type IN ('亲子') THEN '亲子'
        WHEN series_type IN ('纪实') THEN '纪实'
        ELSE series_type END
      ) AS video_type
    FROM knowyou_ott_ods.dim_pub_video_df
    WHERE dt = '${C_DAY}' 
  ) a
  WHERE video_type IN ('电视台', '电影', '电视剧', '少儿', '综艺', '音乐', '体育', '教育', '片花', '纪录片', '娱乐', '时尚', '搞笑', '游戏', '新闻', '生活', '动漫', '其他', '测试', '党建', '电竞', '购物', '亲子', '纪实')
) t3
ON t1.series = t3.series
LEFT JOIN
(
  SELECT 
    series,
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE((
                  CASE WHEN series_director IN ('', 'NULL', '无', '未知', '空', '佚名', 'None') THEN '未知'
                  WHEN series_director IS NULL THEN '未知'
                  ELSE series_director END
                ), ',', '|')
              , '，', '|')
            , '、', '|')
          , '（.*\\)', '')
        , '\\(.*）', '')
      , '\\(.*\\)', '')
    , '（.*）', '') director
  FROM knowyou_ott_ods.dim_pub_video_df
  WHERE dt = '${C_DAY}'
  GROUP BY series, series_director
) t4
ON t1.series = t4.series
LEFT JOIN
(
  SELECT 
    series,
    REGEXP_REPLACE(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE((
                  CASE WHEN series_actordisplay IN ('', 'NULL', '无', '未知', '空', '佚名', 'None', '暂无', '暂无描述') THEN '未知'
                  WHEN series_actordisplay IS NULL THEN '未知'
                  ELSE series_actordisplay END
                ), ',', '|')
              , '，', '|')
            , '、', '|')
          , '（.*\\)', '')
        , '\\(.*）', '')
      , '\\(.*\\)', '')
    , '（.*）', '') actor
  FROM knowyou_ott_ods.dim_pub_video_df
  WHERE dt = '${C_DAY}'
  GROUP BY series, series_actordisplay
) t5
ON t1.series = t5.series
LEFT JOIN
(
  SELECT
    series,
    CONCAT_WS('|', COLLECT_SET(regin)) regin
  FROM
  (
    SELECT
      series, 
      CASE WHEN regin IN ('中国', '中国大陆', '内地', '大陆', '地方', '内地剧', '中国内地', '国内', '华语', '普通话') THEN '中国大陆'
      WHEN regin IN ('中国香港', '香港', '港台', '港澳') THEN '中国香港'
      WHEN regin IN ('台湾', '中国台湾') THEN '中国台湾'
      WHEN regin IN ('日本', '日韩', '日文', '日语') THEN '日本'
      WHEN regin IN ('韩国', '韩日', '韩剧', '韩文') THEN '韩国'
      WHEN regin IN ('泰国', '泰剧', '泰语') THEN '泰国'
      WHEN regin IN ('美国', '欧美', '美剧', '好莱坞', '纽约', '加利福尼亚') THEN '美国'
      WHEN regin IN ('英国', '英语', '伦敦') THEN '英国'
      WHEN regin IN ('法国', '巴黎', '法语') THEN '法国'
      WHEN regin IN ('德国', '西德', '德语') THEN '德国'
      WHEN regin IN ('西班牙', '西班牙语', '马德里') THEN '西班牙'
      WHEN regin IN ('意大利', '意大利语', '罗马') THEN '意大利'
      WHEN regin IN ('芬兰','瑞典','挪威','冰岛','丹麦','爱沙尼亚','拉脱维亚','立陶宛','白俄罗斯','乌克兰','摩尔多瓦','波兰','捷克','斯洛伐克','匈牙利','奥地利','瑞士','列支敦士登','罗马尼亚','保加利亚','塞尔维亚','马其顿','阿尔巴尼亚','希腊','斯洛文尼亚','克罗地亚','波斯尼亚','梵蒂冈','圣马力诺','马耳他','葡萄牙','安道尔','爱尔兰','荷兰','比利时','卢森堡','摩纳哥','墨塞哥维那','欧洲') THEN '欧洲'
      WHEN regin IN ('印度', '印度语') THEN '印度'
      WHEN regin IN ('NULL', '空', '未知', '不详', '') THEN '未知'
      WHEN regin IS NULL THEN '未知'
      ELSE '其他' END regin
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(series_originalcountry, ' ', '|')
                , '\\|\\/\\|', '|')
              , '/', '|')
            , ',', '|')
          , '，', '|')
        , '、', '|'), '\\|')) col AS regin
    WHERE dt = '${C_DAY}' 
  ) a
  GROUP BY series
) t6
ON t1.series = t6.series
LEFT JOIN
(
  SELECT
    series,
    CONCAT_WS('|', COLLECT_SET(language)) language
  FROM
  (
    SELECT
      series, 
      CASE WHEN language IN ('国语', '中文', '汉语', '普通', '普通话普通话', '汉语普通话', '普普通话', '国语普通话', '普通户', '普通话版', '普', '普通话') THEN '普通话'
      WHEN language IN ('方言','粤语','闽南语','陕西话','河南话','蒙古语','藏语','东北话','兴化方言','四川话','山西话','方言','东北方言','湖南话','山东话','上海话','北京话') THEN '方言'
      WHEN language IN ('英语', '英文') THEN '英语'
      WHEN language IN ('日语', '日文') THEN '日语'
      WHEN language IN ('韩语', '韩文') THEN '韩语'
      WHEN language IN ('泰语', '泰文') THEN '泰语'
      WHEN language IN ('法语', '法文') THEN '法语'
      WHEN language IN ('德语', '德文') THEN '德语'
      WHEN language IN ('西班牙语', '西班牙语 阿根廷') THEN '西班牙语'
      WHEN language IN ('意大利语') THEN '意大利语'
      WHEN language IN ('印度语', '北印度语') THEN '印度语'
      WHEN language IN ('NULL', '不详', '\\|', '未知') THEN '未知'
      ELSE '其他' END language
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          REGEXP_REPLACE(
            REGEXP_REPLACE(
              REGEXP_REPLACE(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(series_language, '中\\|文', '|')
                  , '国\\|语', '|')
                , '普\\|通\\|话', '|')
              , '/', '|')
            , ',', '|')
          , '，', '|')
        , '、', '|'), '\\|')) col AS language
    WHERE dt = '${C_DAY}' 
  ) a
  GROUP BY series
) t7
ON t1.series = t7.series
LEFT JOIN
(
  SELECT series, plot 
  FROM knowyou_ott_dmt.guess_like_media_plot
  WHERE dt = '${C_DAY}'
) t8
ON t1.series = t8.series;