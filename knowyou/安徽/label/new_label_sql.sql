-- device_id & user_sub_count
-- 1.1 行为标签-用户活跃信息-基础信息标签表（当日/当月）
-- 1.2 行为标签-用户活跃信息-点播、直播、回看时段标签表（当日/当月）
-- 1.3 行为标签-用户活跃信息-收看CCTV频道标签表（当日/当月）
-- 1.4 行为标签-用户活跃信息-收看点播栏目标签表（当日/当月）
-- 1.5 行为标签-用户回流信息-7日、14日、30日、上月回流用户
-- 1.6 行为标签-用户沉默信息-7日、14日、30日、上月沉默用户
-- 1.7 行为标签-收视偏好-类别偏好-各点播类别（电影、电视剧、综艺、少儿）的轻、中、重偏好
-- 1.8 行为标签-收视偏好-内容偏好-各点播类别（电影、电视剧、综艺、少儿）1000个热门内容的轻、中、重偏好
-- 1.9 行为标签-收视偏好-内容属性-当日是否看过xx类型内容 ？区分点直回吗
-- 1.10 行为标签-行为标签-用户搜索-搜索热词TOP10（当日/当月）
    -- + search_top1_day    | word/videoname| id | subcount | dt +
    -- | search_top2_day    | word/videoname| id | subcount | dt |
    -- | search_top1_month  | word/videoname| id | subcount | dt |
    -- | search_top2_month  | word/videoname| id | subcount | dt |
    
    -- | collect_top2_day   | word/videoname| id | subcount | dt |
    -- | collect_top2_day   | word/videoname| id | subcount | dt |
    -- | collect_top1_month | word/videoname| id | subcount | dt |
    -- + collect_top2_month | word/videoname| id | subcount | dt +

-- 1.11 行为标签-用户订购信息-付费内容订购退订标签（当日/当月）
-- 1.12 行为标签-用户订购信息-ARPU值


-- 1.1 行为标签-用户活跃信息-基础信息标签表（当日/当月）
CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_serv_basic_part1`( 
  `device_id`             STRING  COMMENT '设备id', 
  `is_boot`               STRING  COMMENT '当日是否开机', 
  `is_play`               STRING  COMMENT '当日是否播放', 
  `boot_num`              STRING  COMMENT '当日开机次数', 
  `play_duration`         STRING  COMMENT '当日播放时长', 
  `play_num`              STRING  COMMENT '当日播放次数', 
  `is_live`               STRING  COMMENT '当日收看直播', 
  `live_play_num`         STRING  COMMENT '当日收看直播次数', 
  `live_play_duration`    STRING  COMMENT '当日收看直播时长', 
  `is_demand`             STRING  COMMENT '当日收看点播', 
  `demand_play_num`       STRING  COMMENT '当日收看点播次数', 
  `demand_play_duration`  STRING  COMMENT '当日收看点播时长', 
  `is_replay`             STRING  COMMENT '当日收看回看', 
  `replay_play_num`       STRING  COMMENT '当日收看回看次数', 
  `replay_play_duration`  STRING  COMMENT '当日收看回看时长'
)
COMMENT '当日用户活跃信息基础标签表'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_serv_basic_part2`( 
  `device_id`                   STRING  COMMENT '设备id',
  `month_is_boot`               STRING  COMMENT '当月是否开机', 
  `month_is_play`               STRING  COMMENT '当月是否播放', 
  `month_boot_num`              STRING  COMMENT '当月开机次数', 
  `month_play_duration`         STRING  COMMENT '当月播放时长', 
  `month_play_num`              STRING  COMMENT '当月播放次数', 
  `month_is_live`               STRING  COMMENT '当月收看直播', 
  `month_live_play_num`         STRING  COMMENT '当月收看直播次数', 
  `month_live_play_duration`    STRING  COMMENT '当月收看直播时长', 
  `month_is_demand`             STRING  COMMENT '当月收看点播', 
  `month_demand_play_num`       STRING  COMMENT '当月收看点播次数', 
  `month_demand_play_duration`  STRING  COMMENT '当月收看点播时长', 
  `month_is_replay`             STRING  COMMENT '当月收看回看', 
  `month_replay_play_num`       STRING  COMMENT '当月收看回看次数', 
  `month_replay_play_duration`  STRING  COMMENT '当月收看回看时长'
)
COMMENT '当月用户活跃信息基础标签表'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_serv_basic_total`( 
  `device_id`                   STRING  COMMENT '设备id', 
  `user_sub_id`                 STRING  COMMENT '子账号id',
  `is_boot`                     STRING  COMMENT '当日是否开机', 
  `is_play`                     STRING  COMMENT '当日是否播放', 
  `boot_num`                    STRING  COMMENT '当日开机次数', 
  `play_duration`               STRING  COMMENT '当日播放时长', 
  `play_num`                    STRING  COMMENT '当日播放次数', 
  `is_live`                     STRING  COMMENT '当日收看直播', 
  `live_play_num`               STRING  COMMENT '当日收看直播次数', 
  `live_play_duration`          STRING  COMMENT '当日收看直播时长', 
  `is_demand`                   STRING  COMMENT '当日收看点播', 
  `demand_play_num`             STRING  COMMENT '当日收看点播次数', 
  `demand_play_duration`        STRING  COMMENT '当日收看点播时长', 
  `is_replay`                   STRING  COMMENT '当日收看回看', 
  `replay_play_num`             STRING  COMMENT '当日收看回看次数', 
  `replay_play_duration`        STRING  COMMENT '当日收看回看时长',
  `month_is_boot`               STRING  COMMENT '当月是否开机', 
  `month_is_play`               STRING  COMMENT '当月是否播放', 
  `month_boot_num`              STRING  COMMENT '当月开机次数', 
  `month_play_duration`         STRING  COMMENT '当月播放时长', 
  `month_play_num`              STRING  COMMENT '当月播放次数', 
  `month_is_live`               STRING  COMMENT '当月收看直播', 
  `month_live_play_num`         STRING  COMMENT '当月收看直播次数', 
  `month_live_play_duration`    STRING  COMMENT '当月收看直播时长', 
  `month_is_demand`             STRING  COMMENT '当月收看点播', 
  `month_demand_play_num`       STRING  COMMENT '当月收看点播次数', 
  `month_demand_play_duration`  STRING  COMMENT '当月收看点播时长', 
  `month_is_replay`             STRING  COMMENT '当月收看回看', 
  `month_replay_play_num`       STRING  COMMENT '当月收看回看次数', 
  `month_replay_play_duration`  STRING  COMMENT '当月收看回看时长'
) 
COMMENT '用户活跃信息基础标签表'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


-- 1.2 行为标签-用户活跃信息-点播、直播、回看时段标签表（当日/当月）
CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_demand_prefer_part1`(
  `device_id`               STRING  COMMENT '设备id', 
  `demand_0_is_watched`     STRING  COMMENT '当日时段0-6点是否点播', 
  `demand_6_is_watched`     STRING  COMMENT '当日时段6-9点是否点播', 
  `demand_9_is_watched`     STRING  COMMENT '当日时段9-12点是否点播', 
  `demand_12_is_watched`    STRING  COMMENT '当日时段12-14点是否点播', 
  `demand_14_is_watched`    STRING  COMMENT '当日时段14-18点是否点播', 
  `demand_18_is_watched`    STRING  COMMENT '当日时段18-22点是否点播', 
  `demand_22_is_watched`    STRING  COMMENT '当日时段22-24点是否点播'
)
COMMENT '用户当日点播时段偏好标签表'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_demand_prefer_part2`(
  `device_id`                     STRING  COMMENT '设备id', 
  `month_demand_0_is_watched`     STRING  COMMENT '当月时段0-6点是否点播', 
  `month_demand_6_is_watched`     STRING  COMMENT '当月时段6-9点是否点播', 
  `month_demand_9_is_watched`     STRING  COMMENT '当月时段9-12点是否点播', 
  `month_demand_12_is_watched`    STRING  COMMENT '当月时段12-14点是否点播', 
  `month_demand_14_is_watched`    STRING  COMMENT '当月时段14-18点是否点播', 
  `month_demand_18_is_watched`    STRING  COMMENT '当月时段18-22点是否点播', 
  `month_demand_22_is_watched`    STRING  COMMENT '当月时段22-24点是否点播'
)
COMMENT '用户当月点播时段偏好标签表'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_live_prefer_part1`(
  `device_id`             STRING  COMMENT '设备id', 
  `live_0_is_watched`     STRING  COMMENT '当日时段0-6点是否观看直播', 
  `live_6_is_watched`     STRING  COMMENT '当日时段6-9点是否观看直播', 
  `live_9_is_watched`     STRING  COMMENT '当日时段9-12点是否观看直播', 
  `live_12_is_watched`    STRING  COMMENT '当日时段12-14点是否观看直播', 
  `live_14_is_watched`    STRING  COMMENT '当日时段14-18点是否观看直播', 
  `live_18_is_watched`    STRING  COMMENT '当日时段18-22点是否观看直播', 
  `live_22_is_watched`    STRING  COMMENT '当日时段22-24点是否观看直播'
)
COMMENT '用户当日直播时段偏好标签表'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_live_prefer_part2`(
  `device_id`                   STRING  COMMENT '设备id', 
  `month_live_0_is_watched`     STRING  COMMENT '当月时段0-6点是否观看直播', 
  `month_live_6_is_watched`     STRING  COMMENT '当月时段6-9点是否观看直播', 
  `month_live_9_is_watched`     STRING  COMMENT '当月时段9-12点是否观看直播', 
  `month_live_12_is_watched`    STRING  COMMENT '当月时段12-14点是否观看直播', 
  `month_live_14_is_watched`    STRING  COMMENT '当月时段14-18点是否观看直播', 
  `month_live_18_is_watched`    STRING  COMMENT '当月时段18-22点是否观看直播', 
  `month_live_22_is_watched`    STRING  COMMENT '当月时段22-24点是否观看直播'
)
COMMENT '用户当月直播时段偏好标签表'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'; 
  

CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_replay_prefer_part1`( 
  `device_id`              STRING COMMENT '设备id', 
  `replay_0_is_watched`    STRING  COMMENT '当日时段0-6点是否回看', 
  `replay_6_is_watched`    STRING  COMMENT '当日时段6-9点是否回看', 
  `replay_9_is_watched`    STRING  COMMENT '当日时段9-12点是否回看', 
  `replay_12_is_watched`   STRING  COMMENT '当日时段12-14点是否回看', 
  `replay_14_is_watched`   STRING  COMMENT '当日时段14-18点是否回看', 
  `replay_18_is_watched`   STRING  COMMENT '当日时段18-22点是否回看', 
  `replay_22_is_watched`   STRING  COMMENT '当日时段22-24点是否回看'
)
COMMENT '用户回看时段偏好标签表'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_replay_prefer_part2`( 
  `device_id`                     STRING  COMMENT '设备id', 
  `month_replay_0_is_watched`     STRING  COMMENT '当月时段0-6点是否回看', 
  `month_replay_6_is_watched`     STRING  COMMENT '当月时段6-9点是否回看', 
  `month_replay_9_is_watched`     STRING  COMMENT '当月时段9-12点是否回看', 
  `month_replay_12_is_watched`    STRING  COMMENT '当月时段12-14点是否回看', 
  `month_replay_14_is_watched`    STRING  COMMENT '当月时段14-18点是否回看', 
  `month_replay_18_is_watched`    STRING  COMMENT '当月时段18-22点是否回看', 
  `month_replay_22_is_watched`    STRING  COMMENT '当月时段22-24点是否回看'
)
COMMENT '用户当月回看时段偏好标签表'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_demand_live_replay_total`(
  `device_id`                     STRING  COMMENT '设备id', 
  `user_sub_id`                   STRING  COMMENT '子账号id',
  `demand_0_is_watched`           STRING  COMMENT '当日时段0-6点是否点播', 
  `demand_6_is_watched`           STRING  COMMENT '当日时段6-9点是否点播', 
  `demand_9_is_watched`           STRING  COMMENT '当日时段9-12点是否点播', 
  `demand_12_is_watched`          STRING  COMMENT '当日时段12-14点是否点播', 
  `demand_14_is_watched`          STRING  COMMENT '当日时段14-18点是否点播', 
  `demand_18_is_watched`          STRING  COMMENT '当日时段18-22点是否点播', 
  `demand_22_is_watched`          STRING  COMMENT '当日时段22-24点是否点播',
  `month_demand_0_is_watched`     STRING  COMMENT '当月时段0-6点是否点播', 
  `month_demand_6_is_watched`     STRING  COMMENT '当月时段6-9点是否点播', 
  `month_demand_9_is_watched`     STRING  COMMENT '当月时段9-12点是否点播', 
  `month_demand_12_is_watched`    STRING  COMMENT '当月时段12-14点是否点播', 
  `month_demand_14_is_watched`    STRING  COMMENT '当月时段14-18点是否点播', 
  `month_demand_18_is_watched`    STRING  COMMENT '当月时段18-22点是否点播', 
  `month_demand_22_is_watched`    STRING  COMMENT '当月时段22-24点是否点播',
  `live_0_is_watched`             STRING  COMMENT '当日时段0-6点是否观看直播', 
  `live_6_is_watched`             STRING  COMMENT '当日时段6-9点是否观看直播', 
  `live_9_is_watched`             STRING  COMMENT '当日时段9-12点是否观看直播', 
  `live_12_is_watched`            STRING  COMMENT '当日时段12-14点是否观看直播', 
  `live_14_is_watched`            STRING  COMMENT '当日时段14-18点是否观看直播', 
  `live_18_is_watched`            STRING  COMMENT '当日时段18-22点是否观看直播', 
  `live_22_is_watched`            STRING  COMMENT '当日时段22-24点是否观看直播',
  `month_live_0_is_watched`       STRING  COMMENT '当月时段0-6点是否观看直播', 
  `month_live_6_is_watched`       STRING  COMMENT '当月时段6-9点是否观看直播', 
  `month_live_9_is_watched`       STRING  COMMENT '当月时段9-12点是否观看直播', 
  `month_live_12_is_watched`      STRING  COMMENT '当月时段12-14点是否观看直播', 
  `month_live_14_is_watched`      STRING  COMMENT '当月时段14-18点是否观看直播', 
  `month_live_18_is_watched`      STRING  COMMENT '当月时段18-22点是否观看直播', 
  `month_live_22_is_watched`      STRING  COMMENT '当月时段22-24点是否观看直播',
  `replay_0_is_watched`           STRING  COMMENT '当日时段0-6点是否回看', 
  `replay_6_is_watched`           STRING  COMMENT '当日时段6-9点是否回看', 
  `replay_9_is_watched`           STRING  COMMENT '当日时段9-12点是否回看', 
  `replay_12_is_watched`          STRING  COMMENT '当日时段12-14点是否回看', 
  `replay_14_is_watched`          STRING  COMMENT '当日时段14-18点是否回看', 
  `replay_18_is_watched`          STRING  COMMENT '当日时段18-22点是否回看', 
  `replay_22_is_watched`          STRING  COMMENT '当日时段22-24点是否回看',
  `month_replay_0_is_watched`     STRING  COMMENT '当月时段0-6点是否回看', 
  `month_replay_6_is_watched`     STRING  COMMENT '当月时段6-9点是否回看', 
  `month_replay_9_is_watched`     STRING  COMMENT '当月时段9-12点是否回看', 
  `month_replay_12_is_watched`    STRING  COMMENT '当月时段12-14点是否回看', 
  `month_replay_14_is_watched`    STRING  COMMENT '当月时段14-18点是否回看', 
  `month_replay_18_is_watched`    STRING  COMMENT '当月时段18-22点是否回看', 
  `month_replay_22_is_watched`    STRING  COMMENT '当月时段22-24点是否回看'
  )
COMMENT '用户点直回各时段收看标签表'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


-- 1.3 行为标签-用户活跃信息-收看CCTV频道标签表（当日/当月）
CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_channel_dm_part1`(
  `device_id`        STRING  COMMENT '设备id', 
  `channel_name`     STRING  COMMENT '直播频道名称', 
  `play_duration`    STRING  COMMENT '时长（秒）'
)
COMMENT '当日直播频道中间表'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_channel_dm_part2`(
  `device_id`        STRING  COMMENT '设备id', 
  `channel_name`     STRING  COMMENT '直播频道名称', 
  `play_duration`    STRING  COMMENT '时长（秒）'
)
COMMENT '当月直播频道中间表'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_channel_prefer_cctv_dm_total`( 
  `device_id`                  STRING  COMMENT '设备id', 
  `user_sub_id`                STRING  COMMENT '子账号id',
  `cctv1_is_watched`           STRING  COMMENT '当日是否收看cctv1', 
  `cctv2_is_watched`           STRING  COMMENT '当日是否收看cctv2', 
  `cctv3_is_watched`           STRING  COMMENT '当日是否收看cctv3', 
  `cctv4_is_watched`           STRING  COMMENT '当日是否收看cctv4', 
  `cctv5_is_watched`           STRING  COMMENT '当日是否收看cctv5', 
  `cctv6_is_watched`           STRING  COMMENT '当日是否收看cctv6', 
  `cctv7_is_watched`           STRING  COMMENT '当日是否收看cctv7', 
  `cctv8_is_watched`           STRING  COMMENT '当日是否收看cctv8', 
  `cctv9_is_watched`           STRING  COMMENT '当日是否收看cctv9', 
  `cctv10_is_watched`          STRING  COMMENT '当日是否收看cctv10', 
  `cctv11_is_watched`          STRING  COMMENT '当日是否收看cctv11', 
  `cctv12_is_watched`          STRING  COMMENT '当日是否收看cctv12', 
  `cctv13_is_watched`          STRING  COMMENT '当日是否收看cctv13', 
  `cctv14_is_watched`          STRING  COMMENT '当日是否收看cctv14', 
  `cctv15_is_watched`          STRING  COMMENT '当日是否收看cctv15', 
  `month_cctv1_is_watched`     STRING  COMMENT '当月是否收看cctv1', 
  `month_cctv2_is_watched`     STRING  COMMENT '当月是否收看cctv2', 
  `month_cctv3_is_watched`     STRING  COMMENT '当月是否收看cctv3', 
  `month_cctv4_is_watched`     STRING  COMMENT '当月是否收看cctv4', 
  `month_cctv5_is_watched`     STRING  COMMENT '当月是否收看cctv5', 
  `month_cctv6_is_watched`     STRING  COMMENT '当月是否收看cctv6', 
  `month_cctv7_is_watched`     STRING  COMMENT '当月是否收看cctv7', 
  `month_cctv8_is_watched`     STRING  COMMENT '当月是否收看cctv8', 
  `month_cctv9_is_watched`     STRING  COMMENT '当月是否收看cctv9', 
  `month_cctv10_is_watched`    STRING  COMMENT '当月是否收看cctv10', 
  `month_cctv11_is_watched`    STRING  COMMENT '当月是否收看cctv11', 
  `month_cctv12_is_watched`    STRING  COMMENT '当月是否收看cctv12', 
  `month_cctv13_is_watched`    STRING  COMMENT '当月是否收看cctv13', 
  `month_cctv14_is_watched`    STRING  COMMENT '当月是否收看cctv14', 
  `month_cctv15_is_watched`    STRING  COMMENT '当月是否收看cctv15'
)
COMMENT '直播CCTV频道收看标签表'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


-- 1.4 行为标签-用户活跃信息-收看点播栏目标签表（当日/当月）
CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_demand_type_watched_part1`(
  `device_id`                STRING  COMMENT '设备id', 
  `type_tv_is_watched`       STRING  COMMENT '当日是否收看点播电视剧栏目', 
  `type_film_is_watched`     STRING  COMMENT '当日是否收看点播电影栏目', 
  `type_child_is_watched`    STRING  COMMENT '当日是否收看点播少儿栏目', 
  `type_comic_is_watched`    STRING  COMMENT '当日是否收看点播动漫栏目', 
  `type_sport_is_watched`    STRING  COMMENT '当日是否收看点播体育栏目', 
  `type_doc_is_watched`      STRING  COMMENT '当日是否收看点播纪实栏目', 
  `type_edu_is_watched`      STRING  COMMENT '当日是否收看点播教育栏目',
  `type_game_is_watched`     STRING  COMMENT '当日是否收看点播游戏栏目',
  `type_espo_is_watched`     STRING  COMMENT '当日是否收看点播电竞栏目',
  `type_var_is_watched`      STRING  COMMENT '当日是否收看点播综艺栏目', 
  `type_life_is_watched`     STRING  COMMENT '当日是否收看点播生活栏目'
)
COMMENT '当日点播栏目收看标签表'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_demand_type_watched_part2`(
  `device_id`                      STRING  COMMENT '设备id', 
  `month_type_tv_is_watched`       STRING  COMMENT '当月是否收看点播电视剧栏目', 
  `month_type_film_is_watched`     STRING  COMMENT '当月是否收看点播电影栏目', 
  `month_type_child_is_watched`    STRING  COMMENT '当月是否收看点播少儿栏目', 
  `month_type_comic_is_watched`    STRING  COMMENT '当月是否收看点播动漫栏目', 
  `month_type_sport_is_watched`    STRING  COMMENT '当月是否收看点播体育栏目', 
  `month_type_doc_is_watched`      STRING  COMMENT '当月是否收看点播纪实栏目', 
  `month_type_edu_is_watched`      STRING  COMMENT '当月是否收看点播教育栏目',
  `month_type_game_is_watched`     STRING  COMMENT '当月是否收看点播游戏栏目',
  `month_type_espo_is_watched`     STRING  COMMENT '当月是否收看点播电竞栏目',
  `month_type_var_is_watched`      STRING  COMMENT '当月是否收看点播综艺栏目', 
  `month_type_life_is_watched`     STRING  COMMENT '当月是否收看点播生活栏目'
)
COMMENT '当月点播栏目收看标签表'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_demand_type_watched_total`(
  `device_id`                      STRING  COMMENT '设备id', 
  `user_sub_id`                    STRING  COMMENT '子账号id',
  `type_tv_is_watched`             STRING  COMMENT '当日是否收看电视剧栏目', 
  `type_film_is_watched`           STRING  COMMENT '当日是否收看电影栏目', 
  `type_child_is_watched`          STRING  COMMENT '当日是否收看少儿栏目', 
  `type_comic_is_watched`          STRING  COMMENT '当日是否收看动漫栏目', 
  `type_sport_is_watched`          STRING  COMMENT '当日是否收看体育栏目', 
  `type_doc_is_watched`            STRING  COMMENT '当日是否收看纪实栏目', 
  `type_edu_is_watched`            STRING  COMMENT '当日是否收看教育栏目',
  `type_game_is_watched`           STRING  COMMENT '当日是否收看游戏栏目',
  `type_espo_is_watched`           STRING  COMMENT '当日是否收看电竞栏目',
  `type_var_is_watched`            STRING  COMMENT '当日是否收看综艺栏目', 
  `type_life_is_watched`           STRING  COMMENT '当日是否收看生活栏目', 
  `month_type_tv_is_watched`       STRING  COMMENT '当月是否收看电视剧栏目', 
  `month_type_film_is_watched`     STRING  COMMENT '当月是否收看电影栏目', 
  `month_type_child_is_watched`    STRING  COMMENT '当月是否收看少儿栏目', 
  `month_type_comic_is_watched`    STRING  COMMENT '当月是否收看动漫栏目', 
  `month_type_sport_is_watched`    STRING  COMMENT '当月是否收看体育栏目', 
  `month_type_doc_is_watched`      STRING  COMMENT '当月是否收看纪实栏目', 
  `month_type_edu_is_watched`      STRING  COMMENT '当月是否收看教育栏目',
  `month_type_game_is_watched`     STRING  COMMENT '当月是否收看游戏栏目',
  `month_type_espo_is_watched`     STRING  COMMENT '当月是否收看电竞栏目',
  `month_type_var_is_watched`      STRING  COMMENT '当月是否收看综艺栏目', 
  `month_type_life_is_watched`     STRING  COMMENT '当月是否收看生活栏目'
)
COMMENT '点播栏目收看标签表'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


-- 1.5 行为标签-用户回流信息-7日、14日、30日、上月回流用户
CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_reflow_user`(
  `device_id`                STRING  COMMENT '设备id', 
  `user_sub_id`              STRING  COMMENT '子账号id',
  `pre7_reflow_user`         STRING  COMMENT '7日回流用户标签', 
  `pre14_reflow_user`        STRING  COMMENT '14日回流用户标签', 
  `pre30_reflow_user`        STRING  COMMENT '30日回流用户标签', 
  `pre_month_reflow_user`    STRING  COMMENT '上月回流用户标签'
)
COMMENT '回流用户标签表'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


-- 1.6 行为标签-用户沉默信息-7日、14日、30日、上月沉默用户
CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_silence_user`(
  `device_id`                 STRING  COMMENT '设备id', 
  `user_sub_id`               STRING  COMMENT '子账号id',
  `pre7_silence_user`         STRING  COMMENT '7日沉默用户标签', 
  `pre14_silence_user`        STRING  COMMENT '14日沉默用户标签', 
  `pre30_silence_user`        STRING  COMMENT '30日沉默用户标签', 
  `pre_month_silence_user`    STRING  COMMENT '上月沉默用户标签'
)
COMMENT '沉默用户标签表'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


-- 1.7 行为标签-收视偏好-类别偏好-各点播类别（电影、电视剧、综艺、少儿）的轻、中、重偏好
CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_demand_type_prefer_total`(
  `device_id`                 STRING  COMMENT '设备id', 
  `user_sub_id`               STRING  COMMENT '子账号id',
  `film_prefer_light`         STRING  COMMENT '电影点播类别轻偏好',
  `film_prefer_middle`        STRING  COMMENT '电影点播类别中偏好',
  `film_prefer_high`          STRING  COMMENT '电影点播类别重偏好',
  `tv_prefer_light`           STRING  COMMENT '电视剧点播类别轻偏好',
  `tv_prefer_middle`          STRING  COMMENT '电视剧点播类别中偏好',
  `tv_prefer_high`            STRING  COMMENT '电视剧点播类别重偏好',
  `var_prefer_light`          STRING  COMMENT '综艺点播类别轻偏好',
  `var_prefer_middle`         STRING  COMMENT '综艺点播类别中偏好',
  `var_prefer_high`           STRING  COMMENT '综艺点播类别重偏好',
  `child_prefer_light`        STRING  COMMENT '少儿点播类别轻偏好',
  `child_prefer_middle`       STRING  COMMENT '少儿点播类别中偏好',
  `child_prefer_high`         STRING  COMMENT '少儿点播类别重偏好'
)
COMMENT '各点播类别（电影、电视剧、综艺、少儿）的轻、中、重偏好'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


-- 1.8 行为标签-收视偏好-内容偏好-各点播类别（电影、电视剧、综艺、少儿）1000个热门内容的轻、中、重偏好
CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_demand_film_content_prefer`(
    `device_id`                   STRING  COMMENT '设备id', 
    `user_sub_id`                 STRING  COMMENT '子账号id',
    `programname`                 STRING  COMMENT '节目名', 
    `film_content_prefer_light`   STRING  COMMENT '内容轻偏好',
    `film_content_prefer_middle`  STRING  COMMENT '内容中偏好',
    `film_content_prefer_high`    STRING  COMMENT '内容重偏好'
)
COMMENT '电影1000个热门内容的轻、中、重偏好'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_demand_tv_content_prefer`(
    `device_id`                 STRING  COMMENT '设备id', 
    `user_sub_id`               STRING  COMMENT '子账号id',
    `programname`               STRING  COMMENT '节目名', 
    `tv_content_prefer_light`   STRING  COMMENT '内容轻偏好',
    `tv_content_prefer_middle`  STRING  COMMENT '内容中偏好',
    `tv_content_prefer_high`    STRING  COMMENT '内容重偏好'
)
COMMENT '电视剧1000个热门内容的轻、中、重偏好'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_demand_var_content_prefer`(
    `device_id`                  STRING  COMMENT '设备id', 
    `user_sub_id`                STRING  COMMENT '子账号id',
    `programname`                STRING  COMMENT '节目名', 
    `var_content_prefer_light`   STRING  COMMENT '内容轻偏好',
    `var_content_prefer_middle`  STRING  COMMENT '内容中偏好',
    `var_content_prefer_high`    STRING  COMMENT '内容重偏好'
)
COMMENT '综艺1000个热门内容的轻、中、重偏好'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_demand_child_content_prefer`(
    `device_id`                    STRING  COMMENT '设备id', 
    `user_sub_id`                  STRING  COMMENT '子账号id',
    `programname`                  STRING  COMMENT '节目名', 
    `child_content_prefer_light`   STRING  COMMENT '内容轻偏好',
    `child_content_prefer_middle`  STRING  COMMENT '内容中偏好',
    `child_content_prefer_high`    STRING  COMMENT '内容重偏好'
)
COMMENT '少儿1000个热门内容的轻、中、重偏好'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


-- 1.9 行为标签-收视偏好-内容属性-当日是否看过xx类型内容, 不区分点直回
CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_type_prefer_total`(
    `device_id`                     STRING  COMMENT '设备id', 
    `user_sub_id`                   STRING  COMMENT '子账号id',
    `dlr_type_tv_is_watched`        STRING  COMMENT '当日是否收看电视剧栏目', 
    `dlr_type_film_is_watched`      STRING  COMMENT '当日是否收看电影栏目', 
    `dlr_type_var_is_watched`       STRING  COMMENT '当日是否收看综艺栏目', 
    `dlr_type_child_is_watched`     STRING  COMMENT '当日是否收看少儿栏目', 
    `dlr_type_web_is_watched`       STRING  COMMENT '当日是否收看网剧栏目', 
    `dlr_type_micro_is_watched`     STRING  COMMENT '当日是否收看微电影栏目', 
    `dlr_type_short_is_watched`     STRING  COMMENT '当日是否收看短视频栏目', 
    `dlr_type_comic_is_watched`     STRING  COMMENT '当日是否收看动漫栏目', 
    `dlr_type_sport_is_watched`     STRING  COMMENT '当日是否收看体育栏目', 
    `dlr_type_doc_is_watched`       STRING  COMMENT '当日是否收看纪实栏目', 
    `dlr_type_edu_is_watched`       STRING  COMMENT '当日是否收看教育栏目',
    `dlr_type_esport_is_watched`    STRING  COMMENT '当日是否收看电竞栏目',
    `dlr_type_game_is_watched`      STRING  COMMENT '当日是否收看游戏栏目',
    `dlr_type_music_is_watched`     STRING  COMMENT '当日是否收看音乐栏目',
    `dlr_type_enter_is_watched`     STRING  COMMENT '当日是否收看娱乐栏目',
    `dlr_type_drama_is_watched`     STRING  COMMENT '当日是否收看戏曲栏目',
    `dlr_type_life_is_watched`      STRING  COMMENT '当日是否收看生活栏目'
)
COMMENT '当日收看栏目标签表'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


-- 1.10 行为标签-行为标签-用户搜索收藏-搜索收藏热词TOP10（当日/当月）
    -- + search_top1_day    | word/videoname| id | subcount | dt +
    -- | search_top2_day    | word/videoname| id | subcount | dt |
    -- | search_top1_month  | word/videoname| id | subcount | dt |
    -- | search_top2_month  | word/videoname| id | subcount | dt |
    
    -- | collect_top2_day   | word/videoname| id | subcount | dt |
    -- | collect_top2_day   | word/videoname| id | subcount | dt |
    -- | collect_top1_month | word/videoname| id | subcount | dt |
    -- + collect_top2_month | word/videoname| id | subcount | dt +
CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_search_collect_top10_total`(
    `tag`            string comment '搜索收藏标识',
    `content`        string comment '搜索收藏热词',
    `device_id`      String comment '设备id',
    `user_sub_id`    String comment '子账号id'
)
COMMENT '搜索收藏热词TOP10'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';



-- 1.11 行为标签-用户订购信息-付费内容订购退订标签（当日/当月）
CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_subscribe_unsubscribe_part1`(
    device_id                            STRING  COMMENT '设备id',  
    user_sub_id                          STRING  COMMENT '子账号id', 
    user_id                              STRING  COMMENT '宽带绑定号码', 
    bailingkge_subscribe                 STRING  COMMENT '每日百灵K歌付费新增订购', 
    chaojiyingshihuiyuan_subscribe       STRING  COMMENT '每日超级影视会员付费新增订购', 
    baiyingyoushenghuo_subscribe         STRING  COMMENT '每日百映优生活付费新增订购', 
    chaojiyouxihuiyuan_subscribe         STRING  COMMENT '每日超级游戏会员付费新增订购', 
    chaojidianjinghuiyuan_subscribe      STRING  COMMENT '每日超级电竞会员付费新增订购', 
    jiaoyuhuiyuanyoujiao_subscribe       STRING  COMMENT '每日教育会员-幼教付费新增订购', 
    jiaoyuhuiyuanxiaoxue_subscribe       STRING  COMMENT '每日教育会员-小学付费新增订购', 
    jiaoyuhuiyuanchuzhong_subscribe      STRING  COMMENT '每日教育会员-初中付费新增订购', 
    jiaoyuhuiyuangaozhong_subscribe      STRING  COMMENT '每日教育会员-高中付费新增订购', 
    yinyuehuiyuan_subscribe              STRING  COMMENT '每日互联网电视音乐会员付费新增订购', 
    kumiaohuiyuan_subscribe              STRING  COMMENT '每日酷喵会员付费新增订购', 
    yunshiting_subscribe                 STRING  COMMENT '每日云视听付费新增订购', 
    yinheshaoer_subscribe                STRING  COMMENT '每日银河少儿付费新增订购', 
    shengjianyouxi_subscribe             STRING  COMMENT '每日圣剑游戏付费新增订购', 
    migukuaiyou_subscribe                STRING  COMMENT '每日咪咕快游付费新增订购', 
    mengxiangyouxiting_subscribe         STRING  COMMENT '每日梦想游戏厅付费新增订购', 
    xuanjialeyuan_subscribe              STRING  COMMENT '每日炫佳乐园付费新增订购', 
    leyoushijie_subscribe                STRING  COMMENT '每日乐游世界付费新增订购', 
    dianjingfengbao_subscribe            STRING  COMMENT '每日电竞风暴付费新增订购', 
    touhaodianjing_subscribe             STRING  COMMENT '每日头号电竞付费新增订购', 
    xingfujianshentuan_subscribe         STRING  COMMENT '每日幸福健身团付费新增订购', 
    jiuzhoulexue_subscribe               STRING  COMMENT '每日九州乐学付费新增订购', 
    xueersi_subscribe                    STRING  COMMENT '每日学而思付费新增订购', 
    dierketang_subscribe                 STRING  COMMENT '每日第二课堂付费新增订购', 
    zhinengyuyinhuiyuan_subscribe        STRING  COMMENT '每日智能语音会员付费新增订购', 
    bailingkge_unsubscribe               STRING  COMMENT '每日百灵K歌付费退购', 
    chaojiyingshihuiyuan_unsubscribe     STRING  COMMENT '每日超级影视会员付费退购', 
    baiyingyoushenghuo_unsubscribe       STRING  COMMENT '每日百映优生活付费退购', 
    chaojiyouxihuiyuan_unsubscribe       STRING  COMMENT '每日超级游戏会员付费退购', 
    chaojidianjinghuiyuan_unsubscribe    STRING  COMMENT '每日超级电竞会员付费退购', 
    jiaoyuhuiyuanyoujiao_unsubscribe     STRING  COMMENT '每日教育会员-幼教付费退购', 
    jiaoyuhuiyuanxiaoxue_unsubscribe     STRING  COMMENT '每日教育会员-小学付费退购', 
    jiaoyuhuiyuanchuzhong_unsubscribe    STRING  COMMENT '每日教育会员-初中付费退购', 
    jiaoyuhuiyuangaozhong_unsubscribe    STRING  COMMENT '每日教育会员-高中付费退购', 
    yinyuehuiyuan_unsubscribe            STRING  COMMENT '每日互联网电视音乐会员付费退购', 
    kumiaohuiyuan_unsubscribe            STRING  COMMENT '每日酷喵会员付费退购', 
    yunshiting_unsubscribe               STRING  COMMENT '每日云视听付费退购', 
    yinheshaoer_unsubscribe              STRING  COMMENT '每日银河少儿付费退购', 
    shengjianyouxi_unsubscribe           STRING  COMMENT '每日圣剑游戏付费退购', 
    migukuaiyou_unsubscribe              STRING  COMMENT '每日咪咕快游付费退购', 
    mengxiangyouxiting_unsubscribe       STRING  COMMENT '每日梦想游戏厅付费退购', 
    xuanjialeyuan_unsubscribe            STRING  COMMENT '每日炫佳乐园付费退购', 
    leyoushijie_unsubscribe              STRING  COMMENT '每日乐游世界付费退购', 
    dianjingfengbao_unsubscribe          STRING  COMMENT '每日电竞风暴付费退购', 
    touhaodianjing_unsubscribe           STRING  COMMENT '每日头号电竞付费退购', 
    xingfujianshentuan_unsubscribe       STRING  COMMENT '每日幸福健身团付费退购', 
    jiuzhoulexue_unsubscribe             STRING  COMMENT '每日九州乐学付费退购', 
    xueersi_unsubscribe                  STRING  COMMENT '每日学而思付费退购', 
    dierketang_unsubscribe               STRING  COMMENT '每日第二课堂付费退购', 
    zhinengyuyinhuiyuan_unsubscribe      STRING  COMMENT '每日智能语音会员付费退购' 
)
COMMENT '每日付费内容订购退订标签'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_subscribe_unsubscribe_part2`(
    device_id                            STRING  COMMENT '设备id',  
    user_sub_id                          STRING  COMMENT '子账号id', 
    user_id                              STRING  COMMENT '宽带绑定号码', 
    month_bailingkge_subscribe                 STRING  COMMENT '每月百灵K歌付费新增订购', 
    month_chaojiyingshihuiyuan_subscribe       STRING  COMMENT '每月超级影视会员付费新增订购', 
    month_baiyingyoushenghuo_subscribe         STRING  COMMENT '每月百映优生活付费新增订购', 
    month_chaojiyouxihuiyuan_subscribe         STRING  COMMENT '每月超级游戏会员付费新增订购', 
    month_chaojidianjinghuiyuan_subscribe      STRING  COMMENT '每月超级电竞会员付费新增订购', 
    month_jiaoyuhuiyuanyoujiao_subscribe       STRING  COMMENT '每月教育会员-幼教付费新增订购', 
    month_jiaoyuhuiyuanxiaoxue_subscribe       STRING  COMMENT '每月教育会员-小学付费新增订购', 
    month_jiaoyuhuiyuanchuzhong_subscribe      STRING  COMMENT '每月教育会员-初中付费新增订购', 
    month_jiaoyuhuiyuangaozhong_subscribe      STRING  COMMENT '每月教育会员-高中付费新增订购', 
    month_yinyuehuiyuan_subscribe              STRING  COMMENT '每月互联网电视音乐会员付费新增订购', 
    month_kumiaohuiyuan_subscribe              STRING  COMMENT '每月酷喵会员付费新增订购', 
    month_yunshiting_subscribe                 STRING  COMMENT '每月云视听付费新增订购', 
    month_yinheshaoer_subscribe                STRING  COMMENT '每月银河少儿付费新增订购', 
    month_shengjianyouxi_subscribe             STRING  COMMENT '每月圣剑游戏付费新增订购', 
    month_migukuaiyou_subscribe                STRING  COMMENT '每月咪咕快游付费新增订购', 
    month_mengxiangyouxiting_subscribe         STRING  COMMENT '每月梦想游戏厅付费新增订购', 
    month_xuanjialeyuan_subscribe              STRING  COMMENT '每月炫佳乐园付费新增订购', 
    month_leyoushijie_subscribe                STRING  COMMENT '每月乐游世界付费新增订购', 
    month_dianjingfengbao_subscribe            STRING  COMMENT '每月电竞风暴付费新增订购', 
    month_touhaodianjing_subscribe             STRING  COMMENT '每月头号电竞付费新增订购', 
    month_xingfujianshentuan_subscribe         STRING  COMMENT '每月幸福健身团付费新增订购', 
    month_jiuzhoulexue_subscribe               STRING  COMMENT '每月九州乐学付费新增订购', 
    month_xueersi_subscribe                    STRING  COMMENT '每月学而思付费新增订购', 
    month_dierketang_subscribe                 STRING  COMMENT '每月第二课堂付费新增订购', 
    month_zhinengyuyinhuiyuan_subscribe        STRING  COMMENT '每月智能语音会员付费新增订购', 
    month_bailingkge_unsubscribe               STRING  COMMENT '每月百灵K歌付费退购', 
    month_chaojiyingshihuiyuan_unsubscribe     STRING  COMMENT '每月超级影视会员付费退购', 
    month_baiyingyoushenghuo_unsubscribe       STRING  COMMENT '每月百映优生活付费退购', 
    month_chaojiyouxihuiyuan_unsubscribe       STRING  COMMENT '每月超级游戏会员付费退购', 
    month_chaojidianjinghuiyuan_unsubscribe    STRING  COMMENT '每月超级电竞会员付费退购', 
    month_jiaoyuhuiyuanyoujiao_unsubscribe     STRING  COMMENT '每月教育会员-幼教付费退购', 
    month_jiaoyuhuiyuanxiaoxue_unsubscribe     STRING  COMMENT '每月教育会员-小学付费退购', 
    month_jiaoyuhuiyuanchuzhong_unsubscribe    STRING  COMMENT '每月教育会员-初中付费退购', 
    month_jiaoyuhuiyuangaozhong_unsubscribe    STRING  COMMENT '每月教育会员-高中付费退购', 
    month_yinyuehuiyuan_unsubscribe            STRING  COMMENT '每月互联网电视音乐会员付费退购', 
    month_kumiaohuiyuan_unsubscribe            STRING  COMMENT '每月酷喵会员付费退购', 
    month_yunshiting_unsubscribe               STRING  COMMENT '每月云视听付费退购', 
    month_yinheshaoer_unsubscribe              STRING  COMMENT '每月银河少儿付费退购', 
    month_shengjianyouxi_unsubscribe           STRING  COMMENT '每月圣剑游戏付费退购', 
    month_migukuaiyou_unsubscribe              STRING  COMMENT '每月咪咕快游付费退购', 
    month_mengxiangyouxiting_unsubscribe       STRING  COMMENT '每月梦想游戏厅付费退购', 
    month_xuanjialeyuan_unsubscribe            STRING  COMMENT '每月炫佳乐园付费退购', 
    month_leyoushijie_unsubscribe              STRING  COMMENT '每月乐游世界付费退购', 
    month_dianjingfengbao_unsubscribe          STRING  COMMENT '每月电竞风暴付费退购', 
    month_touhaodianjing_unsubscribe           STRING  COMMENT '每月头号电竞付费退购', 
    month_xingfujianshentuan_unsubscribe       STRING  COMMENT '每月幸福健身团付费退购', 
    month_jiuzhoulexue_unsubscribe             STRING  COMMENT '每月九州乐学付费退购', 
    month_xueersi_unsubscribe                  STRING  COMMENT '每月学而思付费退购', 
    month_dierketang_unsubscribe               STRING  COMMENT '每月第二课堂付费退购', 
    month_zhinengyuyinhuiyuan_unsubscribe      STRING  COMMENT '每月智能语音会员付费退购' 
)
COMMENT '每月付费内容订购退订标签'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


-- 1.13 行为标签-用户订购信息-ARPU值
CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.`htv_arpu`(
    arpu_five      STRING  COMMENT '当日arpu是否大于等于5',
    arpu_ten       STRING  COMMENT '当日arpu是否大于等于10',
    arpu_twenty    STRING  COMMENT '当日arpu是否大于等于20',
    arpu_thirty    STRING  COMMENT '当日arpu是否大于等于30' 
)
COMMENT 'arpu值'
PARTITIONED BY (`dt` STRING COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


