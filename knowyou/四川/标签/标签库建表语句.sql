 --1
CREATE TABLE knowyou_ott_dmt.`htv_serv_basic`( 
  `deviceid` string COMMENT '设备id', 
  `is_boot` string COMMENT '当日是否开机', 
  `is_play` string COMMENT '当日是否播放', 
  `play_duration` string COMMENT '当日播放时长', 
  `play_num` string COMMENT '当日播放次数', 
  `is_live` string COMMENT '当日收看直播', 
  `live_play_num` string COMMENT '当日收看直播次数', 
  `live_play_duration` string COMMENT '当日收看直播时长', 
  `is_demand` string COMMENT '当日收看点播', 
  `demand_play_num` string COMMENT '当日收看点播次数', 
  `demand_play_duration` string COMMENT '当日收看点播时长', 
  `is_replay` string COMMENT '当日收看回看', 
  `replay_play_num` string COMMENT '当日收看回看次数', 
  `replay_play_duration` string COMMENT '当日收看回看时长')
COMMENT '用户活跃信息基础标签表'
PARTITIONED BY (`date_time` string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
CREATE TABLE knowyou_ott_dmt.`htv_serv_basic_MONTH`( 
  `deviceid` string COMMENT '设备id', 
  `is_boot` string COMMENT '当月是否开机', 
  `is_play` string COMMENT '当月是否播放', 
  `play_duration` string COMMENT '当月播放时长', 
  `play_num` string COMMENT '当月播放次数', 
  `is_live` string COMMENT '当月收看直播', 
  `live_play_num` string COMMENT '当月收看直播次数', 
  `live_play_duration` string COMMENT '当月收看直播时长', 
  `is_demand` string COMMENT '当月收看点播', 
  `demand_play_num` string COMMENT '当月收看点播次数', 
  `demand_play_duration` string COMMENT '当月收看点播时长', 
  `is_replay` string COMMENT '当月收看回看', 
  `replay_play_num` string COMMENT '当月收看回看次数', 
  `replay_play_duration` string COMMENT '当月收看回看时长')
COMMENT '用户活跃信息基础标签表'
PARTITIONED BY (`date_time` string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

  --2
 CREATE TABLE knowyou_ott_dmt.`htv_live_prefer`(
  `deviceid` string COMMENT '设备id', 
  `live_prefer_lv1` string COMMENT '当日直播时段0-6点偏好', 
  `live_prefer_lv2` string COMMENT '当日直播时段6-9点偏好', 
  `live_prefer_lv3` string COMMENT '当日直播时段9-12点偏好', 
  `live_prefer_lv4` string COMMENT '当日直播时段12-14点偏好', 
  `live_prefer_lv5` string COMMENT '当日直播时段14-18点偏好', 
  `live_prefer_lv6` string COMMENT '当日直播时段18-22点偏好', 
  `live_prefer_lv7` string COMMENT '当日直播时段22-24点偏好')
COMMENT '用户直播时段偏好标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
 
 CREATE TABLE knowyou_ott_dmt.htv_live_prefer_month(
  `deviceid` string COMMENT '设备id', 
  `live_prefer_lv1` string COMMENT '当日直播时段0-6点偏好', 
  `live_prefer_lv2` string COMMENT '当日直播时段6-9点偏好', 
  `live_prefer_lv3` string COMMENT '当日直播时段9-12点偏好', 
  `live_prefer_lv4` string COMMENT '当日直播时段12-14点偏好', 
  `live_prefer_lv5` string COMMENT '当日直播时段14-18点偏好', 
  `live_prefer_lv6` string COMMENT '当日直播时段18-22点偏好', 
  `live_prefer_lv7` string COMMENT '当日直播时段22-24点偏好')
COMMENT '用户直播时段偏好标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'; 
  
  --3
  CREATE TABLE knowyou_ott_dmt.`htv_demand_prefer`(
  `deviceid` string COMMENT '设备id', 
  `demand_prefer_lv1` string COMMENT '当日点播时段0-6点偏好', 
  `demand_prefer_lv2` string COMMENT '当日点播时段6-9点偏好', 
  `demand_prefer_lv3` string COMMENT '当日点播时段9-12点偏好', 
  `demand_prefer_lv4` string COMMENT '当日点播时段12-14点偏好', 
  `demand_prefer_lv5` string COMMENT '当日点播时段14-18点偏好', 
  `demand_prefer_lv6` string COMMENT '当日点播时段18-22点偏好', 
  `demand_prefer_lv7` string COMMENT '当日点播时段22-24点偏好')
COMMENT '用户点播时段偏好标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  CREATE TABLE knowyou_ott_dmt.`htv_demand_prefer_month`(
  `deviceid` string COMMENT '设备id', 
  `demand_prefer_lv1` string COMMENT '当日点播时段0-6点偏好', 
  `demand_prefer_lv2` string COMMENT '当日点播时段6-9点偏好', 
  `demand_prefer_lv3` string COMMENT '当日点播时段9-12点偏好', 
  `demand_prefer_lv4` string COMMENT '当日点播时段12-14点偏好', 
  `demand_prefer_lv5` string COMMENT '当日点播时段14-18点偏好', 
  `demand_prefer_lv6` string COMMENT '当日点播时段18-22点偏好', 
  `demand_prefer_lv7` string COMMENT '当日点播时段22-24点偏好')
COMMENT '用户点播时段偏好标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

  --4
 CREATE TABLE knowyou_ott_dmt.`htv_replay_prefer`( 
  `deviceid` string COMMENT '设备id', 
  `replay_prefer_lv1` string COMMENT '当日回看时段0-6点偏好', 
  `replay_prefer_lv2` string COMMENT '当日回看时段6-9点偏好', 
  `replay_prefer_lv3` string COMMENT '当日回看时段9-12点偏好', 
  `replay_prefer_lv4` string COMMENT '当日回看时段12-14点偏好', 
  `replay_prefer_lv5` string COMMENT '当日回看时段14-18点偏好', 
  `replay_prefer_lv6` string COMMENT '当日回看时段18-22点偏好', 
  `replay_prefer_lv7` string COMMENT '当日回看时段22-24点偏好')
COMMENT '用户回看时段偏好标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
 CREATE TABLE knowyou_ott_dmt.`htv_replay_prefer_month`( 
  `deviceid` string COMMENT '设备id', 
  `replay_prefer_lv1` string COMMENT '当日回看时段0-6点偏好', 
  `replay_prefer_lv2` string COMMENT '当日回看时段6-9点偏好', 
  `replay_prefer_lv3` string COMMENT '当日回看时段9-12点偏好', 
  `replay_prefer_lv4` string COMMENT '当日回看时段12-14点偏好', 
  `replay_prefer_lv5` string COMMENT '当日回看时段14-18点偏好', 
  `replay_prefer_lv6` string COMMENT '当日回看时段18-22点偏好', 
  `replay_prefer_lv7` string COMMENT '当日回看时段22-24点偏好')
COMMENT '用户回看时段偏好标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';  
  
  CREATE TABLE knowyou_ott_dmt.`htv_channel_dm`(
  `deviceid` string COMMENT '设备id', 
  `channelname` string COMMENT '直播频道名称', 
  `play_duration` string COMMENT '时长（秒）')
COMMENT '当日直播频道中间表'
PARTITIONED BY ( 
  `date_time` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

  CREATE TABLE knowyou_ott_dmt.`htv_channel_dm_month`(
  `deviceid` string COMMENT '设备id', 
  `channelname` string COMMENT '直播频道名称', 
  `play_duration` string COMMENT '时长（秒）')
COMMENT '当日直播频道中间表'
PARTITIONED BY ( 
  `date_time` string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
 
 --5
 
CREATE TABLE knowyou_ott_dmt.`htv_channel_prefer_cctv_dm`( 
  `deviceid` string COMMENT '设备id', 
  `channel_prefer_cctv1` string COMMENT '当日cctv1偏好', 
  `channel_low_prefer_cctv1` string COMMENT '当日cctv1轻偏好', 
  `channel_middle_prefer_cctv1` string COMMENT '当日cctv1中偏好', 
  `channel_high_prefer_cctv1` string COMMENT '当日cctv1重偏好', 
  `channel_prefer_cctv2` string COMMENT '当日cctv2偏好', 
  `channel_low_prefer_cctv2` string COMMENT '当日cctv2轻偏好', 
  `channel_middle_prefer_cctv2` string COMMENT '当日cctv2中偏好', 
  `channel_high_prefer_cctv2` string COMMENT '当日cctv2重偏好', 
  `channel_prefer_cctv3` string COMMENT '当日cctv3偏好', 
  `channel_low_prefer_cctv3` string COMMENT '当日cctv3轻偏好', 
  `channel_middle_prefer_cctv3` string COMMENT '当日cctv3中偏好', 
  `channel_high_prefer_cctv3` string COMMENT '当日cctv3重偏好', 
  `channel_prefer_cctv4` string COMMENT '当日cctv4偏好', 
  `channel_low_prefer_cctv4` string COMMENT '当日cctv4轻偏好', 
  `channel_middle_prefer_cctv4` string COMMENT '当日cctv4中偏好', 
  `channel_high_prefer_cctv4` string COMMENT '当日cctv4重偏好', 
  `channel_prefer_cctv5` string COMMENT '当日cctv5偏好', 
  `channel_low_prefer_cctv5` string COMMENT '当日cctv5轻偏好', 
  `channel_middle_prefer_cctv5` string COMMENT '当日cctv5中偏好', 
  `channel_high_prefer_cctv5` string COMMENT '当日cctv5重偏好', 
  `channel_prefer_cctv6` string COMMENT '当日cctv6偏好', 
  `channel_low_prefer_cctv6` string COMMENT '当日cctv6轻偏好', 
  `channel_middle_prefer_cctv6` string COMMENT '当日cctv6中偏好', 
  `channel_high_prefer_cctv6` string COMMENT '当日cctv6重偏好', 
  `channel_prefer_cctv7` string COMMENT '当日cctv7偏好', 
  `channel_low_prefer_cctv7` string COMMENT '当日cctv7轻偏好', 
  `channel_middle_prefer_cctv7` string COMMENT '当日cctv7中偏好', 
  `channel_high_prefer_cctv7` string COMMENT '当日cctv7重偏好', 
  `channel_prefer_cctv8` string COMMENT '当日cctv8偏好', 
  `channel_low_prefer_cctv8` string COMMENT '当日cctv8轻偏好', 
  `channel_middle_prefer_cctv8` string COMMENT '当日cctv8中偏好', 
  `channel_high_prefer_cctv8` string COMMENT '当日cctv8重偏好', 
  `channel_prefer_cctv9` string COMMENT '当日cctv9偏好', 
  `channel_low_prefer_cctv9` string COMMENT '当日cctv9轻偏好', 
  `channel_middle_prefer_cctv9` string COMMENT '当日cctv9中偏好', 
  `channel_high_prefer_cctv9` string COMMENT '当日cctv9重偏好', 
  `channel_prefer_cctv10` string COMMENT '当日cctv10偏好', 
  `channel_low_prefer_cctv10` string COMMENT '当日cctv10轻偏好', 
  `channel_middle_prefer_cctv10` string COMMENT '当日cctv10中偏好', 
  `channel_high_prefer_cctv10` string COMMENT '当日cctv10重偏好', 
  `channel_prefer_cctv11` string COMMENT '当日cctv11偏好', 
  `channel_low_prefer_cctv11` string COMMENT '当日cctv11轻偏好', 
  `channel_middle_prefer_cctv11` string COMMENT '当日cctv11中偏好', 
  `channel_high_prefer_cctv11` string COMMENT '当日cctv11重偏好', 
  `channel_prefer_cctv12` string COMMENT '当日cctv12偏好', 
  `channel_low_prefer_cctv12` string COMMENT '当日cctv12轻偏好', 
  `channel_middle_prefer_cctv12` string COMMENT '当日cctv12中偏好', 
  `channel_high_prefer_cctv12` string COMMENT '当日cctv12重偏好', 
  `channel_prefer_cctv13` string COMMENT '当日cctv13偏好', 
  `channel_low_prefer_cctv13` string COMMENT '当日cctv13轻偏好', 
  `channel_middle_prefer_cctv13` string COMMENT '当日cctv13中偏好', 
  `channel_high_prefer_cctv13` string COMMENT '当日cctv13重偏好', 
  `channel_prefer_cctv14` string COMMENT '当日cctv14偏好', 
  `channel_low_prefer_cctv14` string COMMENT '当日cctv14轻偏好', 
  `channel_middle_prefer_cctv14` string COMMENT '当日cctv14中偏好', 
  `channel_high_prefer_cctv14` string COMMENT '当日cctv14重偏好', 
  `channel_prefer_cctv15` string COMMENT '当日cctv15偏好', 
  `channel_low_prefer_cctv15` string COMMENT '当日cctv15轻偏好', 
  `channel_middle_prefer_cctv15` string COMMENT '当日cctv15中偏好', 
  `channel_high_prefer_cctv15` string COMMENT '当日cctv15重偏好',
  `channel_prefer_cctv16` string COMMENT '当日cctv16偏好', 
  `channel_low_prefer_cctv16` string COMMENT '当日cctv16轻偏好', 
  `channel_middle_prefer_cctv16` string COMMENT '当日cctv16中偏好', 
  `channel_high_prefer_cctv16` string COMMENT '当日cctv16重偏好', 
  `channel_prefer_cctv17` string COMMENT '当日cctv17偏好', 
  `channel_low_prefer_cctv17` string COMMENT '当日cctv17轻偏好', 
  `channel_middle_prefer_cctv17` string COMMENT '当日cctv17中偏好', 
  `channel_high_prefer_cctv17` string COMMENT '当日cctv17重偏好')
COMMENT '当日直播CCTV频道偏好标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
 
--6 
CREATE TABLE knowyou_ott_dmt.`htv_channel_prefer_dm`( 
  `deviceid` string COMMENT '设备id', 
  `channel_prefer_lv1` string COMMENT '江苏卫视偏好', 
  `channel_prefer_lv2` string COMMENT '浙江卫视偏好', 
  `channel_prefer_lv3` string COMMENT '湖南卫视偏好', 
  `channel_prefer_lv4` string COMMENT '北京卫视偏好', 
  `channel_prefer_lv5` string COMMENT '深圳卫视偏好', 
  `channel_prefer_lv6` string COMMENT '辽宁卫视偏好', 
  `channel_prefer_lv7` string COMMENT '湖北卫视偏好', 
  `channel_prefer_lv8` string COMMENT '安徽卫视偏好', 
  `channel_prefer_lv9` string COMMENT '重庆卫视偏好', 
  `channel_prefer_lv10` string COMMENT '天津卫视偏好', 
  `channel_prefer_lv11` string COMMENT '黑龙江卫视偏好', 
  `channel_prefer_lv12` string COMMENT '吉林卫视偏好', 
  `channel_prefer_lv13` string COMMENT '内蒙古卫视偏好', 
  `channel_prefer_lv14` string COMMENT '河北卫视偏好', 
  `channel_prefer_lv15` string COMMENT '河南卫视偏好', 
  `channel_prefer_lv16` string COMMENT '江西卫视偏好', 
  `channel_prefer_lv17` string COMMENT '广东卫视偏好', 
  `channel_prefer_lv18` string COMMENT '广西卫视偏好', 
  `channel_prefer_lv19` string COMMENT '东南卫视偏好', 
  `channel_prefer_lv20` string COMMENT '云南卫视偏好', 
  `channel_prefer_lv21` string COMMENT '贵州卫视偏好', 
  `channel_prefer_lv22` string COMMENT '山西卫视偏好', 
  `channel_prefer_lv23` string COMMENT '山东卫视偏好', 
  `channel_prefer_lv24` string COMMENT '宁夏卫视偏好')
COMMENT '当日直播卫视频道偏好标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  
  --7
CREATE TABLE knowyou_ott_dmt.`htv_demand_type_prefer_dm`(
  `deviceid` string COMMENT '设备id', 
  `demand_type_prefer_lv1` string COMMENT '电视剧栏目偏好', 
  `demand_type_prefer_lv2` string COMMENT '电影栏目偏好', 
  `demand_type_prefer_lv3` string COMMENT '综艺栏目偏好', 
  `demand_type_prefer_lv4` string COMMENT '少儿栏目偏好', 
  `demand_type_prefer_lv5` string COMMENT '网剧栏目偏好', 
  `demand_type_prefer_lv6` string COMMENT '微电影栏目偏好', 
  `demand_type_prefer_lv7` string COMMENT '短视频栏目偏好', 
  `demand_type_prefer_lv8` string COMMENT '动漫栏目偏好', 
  `demand_type_prefer_lv9` string COMMENT '体育栏目偏好', 
  `demand_type_prefer_lv10` string COMMENT '纪录片栏目偏好', 
  `demand_type_prefer_lv11` string COMMENT '教育栏目偏好',
  `demand_type_prefer_lv12` string COMMENT '电竞栏目偏好',
  `demand_type_prefer_lv13` string COMMENT '游戏栏目偏好',
  `demand_type_prefer_lv14` string COMMENT '音乐栏目偏好',
  `demand_type_prefer_lv15` string COMMENT '娱乐栏目偏好',
  `demand_type_prefer_lv16` string COMMENT '戏曲栏目偏好',
  `demand_type_prefer_lv17` string COMMENT '生活栏目偏好')
COMMENT '当日点播栏目偏好标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


CREATE TABLE knowyou_ott_dmt.`htv_demand_column_prefer_dm`(
  `deviceid` string COMMENT '设备id', 
  `demand_type_prefer_lv1` string COMMENT '电视剧栏目偏好', 
  `demand_type_prefer_lv2` string COMMENT '电影栏目偏好', 
  `demand_type_prefer_lv3` string COMMENT '动漫栏目偏好', 
  `demand_type_prefer_lv4` string COMMENT '体育栏目偏好', 
  `demand_type_prefer_lv5` string COMMENT '纪录片栏目偏好', 
  `demand_type_prefer_lv6` string COMMENT '教育影栏目偏好', 
  `demand_type_prefer_lv7` string COMMENT '游戏频栏目偏好', 
  `demand_type_prefer_lv8` string COMMENT '电竞栏目偏好', 
  `demand_type_prefer_lv9` string COMMENT '综艺栏目偏好', 
  `demand_type_prefer_lv10` string COMMENT '生活片栏目偏好', 
  `demand_type_prefer_lv11` string COMMENT '少儿栏目偏好',
  `demand_type_prefer_other` string COMMENT '其他栏目偏好')
COMMENT '当日点播栏目偏好标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  
CREATE TABLE knowyou_ott_dmt.`htv_demand_type_prefer_dm_month`(
  `deviceid` string COMMENT '设备id', 
  `demand_type_prefer_lv1` string COMMENT '电视剧栏目偏好', 
  `demand_type_prefer_lv2` string COMMENT '电影栏目偏好', 
  `demand_type_prefer_lv3` string COMMENT '动漫栏目偏好', 
  `demand_type_prefer_lv4` string COMMENT '体育栏目偏好', 
  `demand_type_prefer_lv5` string COMMENT '纪录片栏目偏好', 
  `demand_type_prefer_lv6` string COMMENT '教育影栏目偏好', 
  `demand_type_prefer_lv7` string COMMENT '游戏频栏目偏好', 
  `demand_type_prefer_lv8` string COMMENT '电竞栏目偏好', 
  `demand_type_prefer_lv9` string COMMENT '综艺栏目偏好', 
  `demand_type_prefer_lv10` string COMMENT '生活片栏目偏好', 
  `demand_type_prefer_lv11` string COMMENT '少儿栏目偏好',
  `demand_type_prefer_other` string COMMENT '其他栏目偏好')
COMMENT '当月点播栏目偏好标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  




  --8
CREATE TABLE knowyou_ott_dmt.`htv_adduserretentionlabe_dm`(
  `deviceid` string COMMENT '设备id', 
  `morrow_activer` string COMMENT '次日新增留存用户', 
  `seven_activer` string COMMENT '7日新增留存用户', 
  `fourteen_activer` string COMMENT '14日新增留存用户', 
  `thirty_activer` string COMMENT '30日新增留存用户')
COMMENT '用户新增留存标签日表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  
  
  --9
CREATE TABLE knowyou_ott_dmt.`htv_activeretentionlabe_dm`(
  `deviceid` string COMMENT '设备id', 
  `morrow_activer` string COMMENT '次日活跃留存用户', 
  `seven_activer` string COMMENT '7日活跃留存用户', 
  `fourteen_activer` string COMMENT '14日活跃留存用户', 
  `thirty_activer` string COMMENT '30日活跃留存用户')
COMMENT '用户活跃留存标签日表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  
  --10
CREATE TABLE knowyou_ott_dmt.`htv_activeadd_retention_mm`(
  `deal_time` string COMMENT '日期', 
  `deviceid` string COMMENT '设备id', 
  `month_activer` string COMMENT '月活跃用户标签', 
  `month_adduer` string COMMENT '月新增留存用户标签')
COMMENT '用户留存标签月表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  
  --11
CREATE TABLE knowyou_ott_dmt.`htv_back_flow_lv7`(
  `deviceid` string COMMENT '设备id')
COMMENT '7日回流用户标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  
  --12
CREATE TABLE knowyou_ott_dmt.`htv_back_flow_lv14`(
  `deviceid` string COMMENT '设备id')
COMMENT '14日回流用户标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  
  
  --13
CREATE TABLE knowyou_ott_dmt.`htv_back_flow_lv30`(
  `deviceid` string COMMENT '设备id')
COMMENT '30日回流用户标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  
  --14
CREATE TABLE knowyou_ott_dmt.`htv_slience_dm`(
  `deviceid` string COMMENT '设备id', 
  `pre7_slience_user` string COMMENT '前7日沉默用户标签', 
  `pre14_slience_user` string COMMENT '前14日沉默用户标签', 
  `pre30_slience_user` string COMMENT '前30日沉默用户标签', 
  `pre_month_slience_user` string COMMENT '上月沉默用户标签')
COMMENT '沉默用户标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  
  --15
CREATE TABLE knowyou_ott_dmt.`htv_live_content_light`(
  `deviceid` string COMMENT '设备id', 
  `videoname` string COMMENT '内容名称')
COMMENT '直播内容100个轻偏好标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  --16
CREATE TABLE knowyou_ott_dmt.`htv_live_content_middle`(
  `deviceid` string COMMENT '设备id', 
  `videoname` string COMMENT '内容名称')
COMMENT '直播内容100个中偏好标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'; 
  
  --17
CREATE TABLE knowyou_ott_dmt.`htv_live_content_high`( 
  `deviceid` string COMMENT '设备id', 
  `videoname` string COMMENT '内容名称')
COMMENT '直播内容100个重偏好标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  

  --15
CREATE TABLE knowyou_ott_dmt.`htv_demand_content_light`(
  `deviceid` string COMMENT '设备id', 
  `videoname` string COMMENT '内容名称')
COMMENT '点播内容100个轻偏好标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  --16
CREATE TABLE knowyou_ott_dmt.`htv_demand_content_middle`(
  `deviceid` string COMMENT '设备id', 
  `videoname` string COMMENT '内容名称')
COMMENT '点播内容100个中偏好标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'; 
  
  --17
CREATE TABLE knowyou_ott_dmt.`htv_demand_content_high`( 
  `deviceid` string COMMENT '设备id', 
  `videoname` string COMMENT '内容名称')
COMMENT '点播内容100个重偏好标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  

CREATE TABLE knowyou_ott_dmt.htv_searchcollectlabe_dm(
  `tag` string COMMENT '标志',
  `videoname` string COMMENT '内容名称',
  `playnums` string COMMENT '数量'
)
COMMENT '搜索收藏分类'
PARTITIONED BY (`date_time` string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';




CREATE TABLE knowyou_ott_dmt.htv_app_basic(
  deviceid string COMMENT '设备id',
  jiguang_pre string COMMENT '当日收看极光TVAPP',  
  jiguang_count string COMMENT '当日收看极光TVAPP的次数', 
  jiguang_time string COMMENT '当日收看极光TVAPP的时长', 
  qiyi_pre string COMMENT '当日收看银河奇异果APP',
  qiyi_count string COMMENT '当日收看银河奇异果APP的次数',
  qiyi_time string COMMENT '当日收看银河奇异果APP的时长',
  bailin_pre string COMMENT '当日收看百灵K歌APP',
  bailin_count string COMMENT '当日收看百灵K歌APP的次数',
  bailin_time string COMMENT '当日收看百灵K歌APP的时长',
  other_pre string COMMENT '当日收看其他APP',
  other_count string COMMENT '当日收看其他APP的次数',
  other_time string COMMENT '当日收看其他APP的时长'
)
COMMENT '当日收看各类APP信息情况表'
PARTITIONED BY (`date_time` string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';  


CREATE TABLE knowyou_ott_dmt.htv_app_basic_month(
  deviceid string COMMENT '设备id',
  jiguang_pre string COMMENT '当月收看极光TVAPP',  
  jiguang_count string COMMENT '当月收看极光TVAPP的次数', 
  jiguang_time string COMMENT '当月收看极光TVAPP的时长', 
  qiyi_pre string COMMENT '当月收看银河奇异果APP',
  qiyi_count string COMMENT '当月收看银河奇异果APP的次数',
  qiyi_time string COMMENT '当月收看银河奇异果APP的时长',
  bailin_pre string COMMENT '当月收看百灵K歌APP',
  bailin_count string COMMENT '当月收看百灵K歌APP的次数',
  bailin_time string COMMENT '当月收看百灵K歌APP的时长',
  other_pre string COMMENT '当月收看其他APP',
  other_count string COMMENT '当月收看其他APP的次数',
  other_time string COMMENT '当月收看其他APP的时长'
)
COMMENT '当月收看各类APP信息情况表'
PARTITIONED BY (`date_time` string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';  



CREATE TABLE knowyou_ott_dmt.htv_app_light(
  deviceid string COMMENT '设备id',
  appname string COMMENT 'APP名'
)
COMMENT 'app收视轻偏好'
PARTITIONED BY (`date_time` string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

CREATE TABLE knowyou_ott_dmt.htv_app_middle(
  deviceid string COMMENT '设备id',
  appname string COMMENT 'APP名'
)
COMMENT 'app收视中偏好'
PARTITIONED BY (`date_time` string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

CREATE TABLE knowyou_ott_dmt.htv_app_high(
  deviceid string COMMENT '设备id',
  appname string COMMENT 'APP名'
)
COMMENT 'app收视重偏好'
PARTITIONED BY (`date_time` string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


CREATE TABLE knowyou_ott_dmt.htv_plotlabel_dm(
deviceid string,
v_b1   string COMMENT '是否看过喜剧内容' ,
v_b2   string COMMENT '是否看过动作内容' ,
v_b3   string COMMENT '是否看过警匪内容' ,
v_b4   string COMMENT '是否看过爱情内容' ,
v_b5   string COMMENT '是否看过青春内容' ,
v_b6   string COMMENT '是否看过犯罪内容' ,
v_b7   string COMMENT '是否看过悬疑内容' ,
v_b8   string COMMENT '是否看过惊悚内容' ,
v_b9   string COMMENT '是否看过恐怖内容' ,
v_b10  string COMMENT '是否看过灾难内容' ,
v_b11  string COMMENT '是否看过战争内容' ,
v_b12  string COMMENT '是否看过科幻内容' ,
v_b13  string COMMENT '是否看过武侠内容' ,
v_b14  string COMMENT '是否看过纪录片内容' ,
v_b15  string COMMENT '是否看过体育内容' ,
v_b16  string COMMENT '是否看过幼儿内容' ,
v_b17  string COMMENT '是否看过动画内容' ,
v_b18  string COMMENT '是否看过励志内容' ,
v_b19  string COMMENT '是否看过戏曲内容' ,
v_b20  string COMMENT '是否看过广场舞内容' ,
v_b21  string COMMENT '是否看过真人秀内容' ,
v_b22  string COMMENT '是否看过偶像内容' ,
v_b23  string COMMENT '是否看过冒险内容' ,
v_b24  string COMMENT '是否看过时尚内容' ,
v_b25  string COMMENT '是否看过言情内容' ,
v_b26  string COMMENT '是否看过足球内容' ,
v_b27  string COMMENT '是否看过篮球内容' ,
v_b28  string COMMENT '是否看过网球内容' ,
v_b29  string COMMENT '是否看过探索内容' ,
v_b30  string COMMENT '是否看过自然内容' ,
v_b31  string COMMENT '是否看过人文内容' ,
v_b32  string COMMENT '是否看过古装内容' ,
v_b33  string COMMENT '是否看过谍战内容' ,
v_b34  string COMMENT '是否看过其他内容' 
)
COMMENT '情节分类媒资标签'
PARTITIONED BY (`date_time` string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


CREATE TABLE knowyou_ott_dmt.htv_regionlabel_dm(
    deviceid string,
    v_b1    string  COMMENT  '看过内地地区内容',
    v_b2    string  COMMENT  '看过香港地区内容',
    v_b3    string  COMMENT  '看过中国台湾地区内容',
    v_b4    string  COMMENT  '看过日本地区内容',
    v_b5    string  COMMENT  '看过韩国地区内容',
    v_b6    string  COMMENT  '看过泰国地区内容',
    v_b7    string  COMMENT  '看过美国地区内容',
    v_b8    string  COMMENT  '看过英国地区内容',
    v_b9    string  COMMENT  '看过意大利地区内容',
    v_b10   string  COMMENT  '看过法国地区内容',
    v_b11   string  COMMENT  '看过俄罗斯地区内容',
    v_b12   string  COMMENT  '看过德国地区内容',
    v_b13   string  COMMENT  '看过印度地区内容',
    v_b14   string  COMMENT  '看过欧洲地区内容',
    v_b15   string,
    v_b16   string,
    v_b17   string,
    v_b18   string,
    v_b19   string,
    v_b20   string
)
COMMENT '地区分类媒资标签'
PARTITIONED BY (`date_time` string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


CREATE TABLE knowyou_ott_dmt.htv_yearslabel_dm(
    deviceid string,
    v_b1    string  COMMENT '看过2022年内容',
    v_b2    string  COMMENT '看过2021年内容',
    v_b3    string  COMMENT '看过2020年内容',
    v_b4    string  COMMENT '看过2019年内容',
    v_b5    string  COMMENT '看过2018年内容',
    v_b6    string  COMMENT '看过2017年内容',
    v_b7    string  COMMENT '看过2010-2016年内容',
    v_b8    string  COMMENT '看过2000-2009年内容',
    v_b9    string  COMMENT '看过90年代内容',
    v_b10   string  COMMENT '看过80年代内容',
    v_b11   string  COMMENT '看过70年代内容',
    v_b12   string  ,
    v_b13   string  ,
    v_b14   string  ,
    v_b15   string  ,
    v_b16   string  ,
    v_b17   string  ,
    v_b18   string  ,
    v_b19   string  ,
    v_b20   string  
)
COMMENT '发行年限分类媒资标签'
PARTITIONED BY (`date_time` string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


CREATE TABLE knowyou_ott_dmt.htv_languagelabel_dm(
    deviceid string,
    v_b1    string  COMMENT '看过中文内容',
    v_b2    string  COMMENT '看过英文内容',
    v_b3    string  COMMENT '看过法语内容',
    v_b4    string  COMMENT '看过德语内容',
    v_b5    string  COMMENT '看过印度语内容',
    v_b6    string  COMMENT '看过日语内容',
    v_b7    string  COMMENT '看过韩语内容',
    v_b8    string  COMMENT '看过泰语内容',
    v_b9    string  ,
    v_b10   string  ,
    v_b11   string  ,
    v_b12   string  ,
    v_b13   string  ,
    v_b14   string  ,
    v_b15   string  ,
    v_b16   string  ,
    v_b17   string  ,
    v_b18   string  ,
    v_b19   string  ,
    v_b20   string  
)
COMMENT '语言分类媒资标签'
PARTITIONED BY (`date_time` string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';



CREATE TABLE knowyou_ott_dmt.htv_actorlabel_dm(
    deviceid string,
    actorname string
)
COMMENT '演员分类媒资标签'
PARTITIONED BY (`date_time` string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


CREATE TABLE knowyou_ott_dmt.htv_directorlabel_dm(
    deviceid string,
    directorname string
)
COMMENT '导演分类媒资标签'
PARTITIONED BY (`date_time` string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.htv_demand_order_dm(
    deviceid         string  COMMENT 'id',
    film_not_order   string  COMMENT '电影收视用户且未订购',
    tv_not_order     string  COMMENT '电视剧收视用户且未订购',
    child_not_order  string  COMMENT '少儿收视用户且未订购'
)
COMMENT '电影/电视剧/少儿收视用户且未订购'
PARTITIONED BY (`date_time` string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.htv_film_order_dm(
    deviceid               string  COMMENT 'id',
    film_light_not_order   string  COMMENT '电影点播类别轻偏好且未订购',
    film_middle_not_order  string  COMMENT '电影点播类别中偏好且未订购',
    film_high_not_order    string  COMMENT '电影点播类别重偏好且未订购'
)
COMMENT '电影点播类别偏好未订购表'
PARTITIONED BY (`date_time` string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.htv_tv_order_dm(
    deviceid             string  COMMENT 'id',
    tv_light_not_order   string  COMMENT '电视剧点播类别轻偏好且未订购',
    tv_middle_not_order  string  COMMENT '电视剧点播类别中偏好且未订购',
    tv_high_not_order    string  COMMENT '电视剧点播类别重偏好且未订购'
)
COMMENT '电视剧点播类别偏好未订购表'
PARTITIONED BY (`date_time` string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


CREATE TABLE IF NOT EXISTS knowyou_ott_dmt.htv_child_order_dm(
    deviceid                string  COMMENT 'id',
    child_light_not_order   string  COMMENT '少儿点播类别轻偏好且未订购',
    child_middle_not_order  string  COMMENT '少儿点播类别中偏好且未订购',
    child_high_not_order    string  COMMENT '少儿点播类别重偏好且未订购'
)
COMMENT '少儿点播类别偏好未订购表'
PARTITIONED BY (`date_time` string COMMENT '日期')
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';



  
CREATE TABLE knowyou_ott_dmt.`htv_epg_prefer_dm`( 
  `deviceid` string COMMENT '设备id', 
  `fivelevel` string COMMENT '点播类别', 
  `hourtype` string COMMENT '分段时间', 
  `ranks` string COMMENT '设备id')
COMMENT '用户画像分时段epg栏目top偏好'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  
  
  
CREATE TABLE knowyou_ott_dmt.`htv_content_prefer_dm`( 
  `deviceid` string COMMENT '设备id', 
  `contenttype` string COMMENT '点播类别', 
  `hourtype` string COMMENT '分段时间', 
  `ranks` string COMMENT '设备id')
COMMENT '用户画像分时段标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  
  
  
      CREATE TABLE knowyou_ott_dmt.`htv_user_feature_dm`( 
  `deviceid` string COMMENT '设备id', 
  `top1_content` string COMMENT '点播类别top1', 
  `top2_content` string COMMENT '点播类别top2', 
  `top3_content` string COMMENT '点播类别top3', 
  `top1_epg` string COMMENT '点播类别epgtop1', 
  `top2_epg` string COMMENT '点播类别epgtop2', 
  `top3_epg` string COMMENT '点播类别epgtop3', 
  `top4_epg` string COMMENT '点播类别epgtop4', 
  `top5_epg` string COMMENT '点播类别epgtop5', 
  `hourtype` string COMMENT '分段时间')
COMMENT '用户特征标签表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  
  
  
  CREATE TABLE knowyou_ott_dmt.`imp_contenttype_grade`( 
  `content_type` string COMMENT '点播类别', 
  `child_rate` string COMMENT '儿童', 
  `teen_rate` string COMMENT '青少年', 
  `youth_rate` string COMMENT '青年', 
  `mid_rate` string COMMENT '中年', 
  `old_rate` string COMMENT '老年', 
  `male_rate` string COMMENT '男性', 
  `female_rate` string COMMENT '女性')
COMMENT '基础权重表' 
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  
insert into table knowyou_ott_dmt.`imp_contenttype_grade` values('少儿',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.`imp_contenttype_grade` values('综艺',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.`imp_contenttype_grade` values('电影',0,0,0.3,0,0,0);
insert into table knowyou_ott_dmt.`imp_contenttype_grade` values('电视剧',0,0.2,0.3,0.3,0,0,0);
insert into table knowyou_ott_dmt.`imp_contenttype_grade` values('动漫',0.1,0.3,0,0,0,0,0);
insert into table knowyou_ott_dmt.`imp_contenttype_grade` values('电竞',0.2,0.3,0.1,0,0,0,0);
insert into table knowyou_ott_dmt.`imp_contenttype_grade` values('生活',0,0,0.2,0.3,0.2,0,0);
insert into table knowyou_ott_dmt.`imp_contenttype_grade` values('纪实',0,0,0.1,0.3,0,0,0);
insert into table knowyou_ott_dmt.`imp_contenttype_grade` values('音乐',0,0,0.2,0.3,0,0,0);
insert into table knowyou_ott_dmt.`imp_contenttype_grade` values('体育',0,0,0.3,0.2,0,0,0);
insert into table knowyou_ott_dmt.`imp_contenttype_grade` values('教育',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.`imp_contenttype_grade` values('新闻',0,0,0,0.3,0.1,0,0);
insert into table knowyou_ott_dmt.`imp_contenttype_grade` values('云游戏',0.1,0.3,0.1,0,0,0,0);
insert into table knowyou_ott_dmt.`imp_contenttype_grade` values('游戏',0.1,0.3,0.1,0,0,0,0);





  CREATE TABLE knowyou_ott_dmt.`imp_epg_type_grade`( 
  `epg` string COMMENT 'epg三级类别', 
  `child_rate` string COMMENT '儿童', 
  `teen_rate` string COMMENT '青少年', 
  `youth_rate` string COMMENT '青年', 
  `mid_rate` string COMMENT '中年', 
  `old_rate` string COMMENT '老年', 
  `male_rate` string COMMENT '男性', 
  `female_rate` string COMMENT '女性')
COMMENT '自定义权重表' 
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  
insert into table knowyou_ott_dmt.imp_epg_type_grade values('全部免费',0,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('综艺',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('电影',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('少儿',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('电视剧',0,0.2,0.3,0.3,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('益智启蒙',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('开心动画',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('0-3岁',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('4-6岁',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('免费专区',0,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('全部少儿',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('古装仙侠',0,0.1,0.3,0.2,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('青春偶像',0,0.3,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('精彩推荐',0,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('高分好评',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('最新热映',0,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('亿元票房',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('恐怖惊悚',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('超清4K专区',0,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('动画电影',0.1,0.3,0.2,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('最新上线',0,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('迷你世界',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('我的世界',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('最近热播',0,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('王者荣耀',0,0.3,0.2,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('英雄联盟',0,0.3,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('吃鸡游戏',0,0.3,0.2,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('汪汪队',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('宝宝巴士',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('超级飞侠',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('熊出没',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('小猪佩奇',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('奥特曼',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('全部综艺',0,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('同步更新',0,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('浪漫爱情',0,0,0.3,0,0,0,0.1);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('热门推荐',0,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('爆笑喜剧',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('动作冒险',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('成长冒险',0.1,0.3,0.1,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('童趣儿歌',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('儿歌故事',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('英语启蒙',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('五年级',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('益智思维',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('二年级',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('一年级',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('拼音识字',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('三年级',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('早教百科',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('全部教育',0.3,0.3,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('四年级',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('兴趣培养',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('六年级',0,0.3,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('都市情缘',0,0,0.3,0.3,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('悬疑惊悚',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('军旅谍战',0,0,0.2,0.3,0,0.1,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('武侠江湖',0,0.1,0.3,0.2,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('家长里短',0,0,0.3,0,0,0,0.1);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('7-10岁',0.3,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('乡里乡亲',0,0,0.3,0,0,0,0.1);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('历史战争',0,0,0.2,0.3,0,0.1,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('武侠玄幻',0,0.1,0.3,0.2,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('舌尖美食',0,0,0.3,0.3,0,0,0.1);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('内地剧场',0,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('年代传奇',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('全部剧集',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('海外剧场',0,0,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('全部动漫',0.1,0.3,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('热门新番',0.1,0.3,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('搞笑治愈',0,0,0.3,0,0,0,0.1);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('历史风云',0,0,0.3,0,0,0.1,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('古墓疑云',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('悬疑烧脑',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('科幻巨制',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('华语院线',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('战争史诗',0,0,0.3,0,0,0.1,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('11-13岁',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('全部电影',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('青春恋爱',0,0.3,0.3,0,0,0,0.1);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('热血冒险',0.1,0.3,0.1,0,0,0.1,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('网络电影',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('饮食指南',0,0,0.3,0.3,0,0,0.1);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('广场舞',0,0,0,0.3,0.3,0,0.1);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('欧美佳片',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('居家旅行',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('好莱坞专区',0,0.3,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('科普养生',0,0,0.3,0.3,0.3,0.1,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('运动健身',0,0,0.3,0.3,0,0,0.1);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('潮流时尚',0,0,0.3,0,0,0,0.1);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('歌舞曲艺',0,0,0.1,0.3,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('影片特辑',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('真人秀场',0,0,0.3,0,0,0,0.1);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('竞技选秀',0,0,0.3,0,0,0,0.1);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('其他热游',0,0.3,0.2,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('手机游戏',0,0.3,0.2,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('休闲益智',0.3,0.1,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('电竞赛事',0,0.3,0.2,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('全部电竞',0,0.3,0.2,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('单机游戏',0,0.3,0.2,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('喜剧搞笑',0,0,0,0.3,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('主播解说',0,0.2,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('网络游戏',0,0.3,0.2,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('生活服务',0,0,0.3,0,0,0,0.1);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('港台剧场',0,0,0.3,0,0,0,0.1);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('野外求生',0,0,0.3,0,0,0.1,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('相声小品',0,0,0.3,0,0,0,0.1);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('戏曲天地',0,0,0,0.3,0.3,0,0.1);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('明星访谈',0,0,0.3,0,0,0,0.1);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('情感伦理',0,0,0.3,0.2,0,0,0.1);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('音乐现场',0,0,0.3,0,0,0,0.1);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('美食生活',0,0,0.3,0.3,0,0,0.1);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('文化休闲',0,0,0.3,0.3,0,0,0.1);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('晚会盛典',0,0,0.3,0,0,0,0.1);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('自然万象',0,0,0.1,0.3,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('篮球',0,0,0.3,0.2,0,0.1,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('足球',0,0,0.3,0.2,0,0.1,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('综合体育',0,0,0.3,0.2,0,0.1,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('功夫搏击',0,0,0.3,0.2,0,0.1,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('冰雪运动',0,0,0.3,0.2,0,0.1,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('全部纪录片',0,0,0.3,0.2,0,0.1,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('全部体育',0,0,0.3,0.2,0,0.1,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('社会纪实',0,0,0.3,0.2,0,0.1,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('人文风情',0,0,0.3,0.2,0,0.1,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('军武纪实',0,0,0,0.3,0.2,0.1,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('排球',0,0,0.3,0.2,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('更多球类',0,0,0.3,0.2,0,0.1,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('人物传记',0,0,0,0.3,0.2,0.1,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('科技刑侦',0,0,0,0.3,0.2,0.1,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('豆瓣高分',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('全部生活',0,0,0.2,0.3,0.2,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('职场技能',0,0,0.3,0.3,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('九年级',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('人文知识',0,0,0.3,0.2,0,0.1,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('八年级',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('大学名校',0,0,0.3,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('艺术修养',0,0,0,0.3,0.3,0.1,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('七年级',0,0.3,0,0,0,0,0);
insert into table knowyou_ott_dmt.imp_epg_type_grade values('动漫',0.1,0.3,0,0,0,0,0);




  CREATE TABLE knowyou_ott_dmt.`htv_user_rating_detail_dm`( 
  `deviceid` string COMMENT '设备id', 
  `hourtype` string COMMENT '时段', 
  `child_top1_content_rate` string COMMENT '儿童socretop1', 
  `teen_top1_content_rate` string COMMENT '青少年socretop1', 
  `youth_top1_content_rate` string COMMENT '青年socretop1', 
  `mid_top1_content_rate` string COMMENT '中年socretop1', 
  `old_top1_content_rate` string COMMENT '老年socretop1', 
  `male_top1_content_rate` string COMMENT '男性socretop1', 
  `female_top1_content_rate` string COMMENT '女性socretop1',
  `child_top2_content_rate` string COMMENT '儿童socretop2', 
  `teen_top2_content_rate` string COMMENT '青少年socretop2', 
  `youth_top2_content_rate` string COMMENT '青年socretop2', 
  `mid_top2_content_rate` string COMMENT '中年socretop2', 
  `old_top2_content_rate` string COMMENT '老年socretop2', 
  `male_top2_content_rate` string COMMENT '男性socretop2', 
  `female_top2_content_rate` string COMMENT '女性socretop2',
  `child_top3_content_rate` string COMMENT '儿童socretop3', 
  `teen_top3_content_rate` string COMMENT '青少年socretop3', 
  `youth_top3_content_rate` string COMMENT '青年socretop3', 
  `mid_top3_content_rate` string COMMENT '中年socretop3', 
  `old_top3_content_rate` string COMMENT '老年socretop3', 
  `male_top3_content_rate` string COMMENT '男性socretop3', 
  `female_top3_content_rate` string COMMENT '女性socretop3',
  `child_top1_epg_rate` string COMMENT '儿童epgsocretop1', 
  `teen_top1_epg_rate` string COMMENT '青少年epgsocretop1', 
  `youth_top1_epg_rate` string COMMENT '青年epgsocretop1', 
  `mid_top1_epg_rate` string COMMENT '中年epgsocretop1', 
  `old_top1_epg_rate` string COMMENT '老年epgsocretop1', 
  `male_top1_epg_rate` string COMMENT '男性epgsocretop1', 
  `female_top1_epg_rate` string COMMENT '女性epgsocretop1',
  `child_top2_epg_rate` string COMMENT '儿童epgsocretop2', 
  `teen_top2_epg_rate` string COMMENT '青少年epgsocretop2', 
  `youth_top2_epg_rate` string COMMENT '青年epgsocretop2', 
  `mid_top2_epg_rate` string COMMENT '中年epgsocretop2', 
  `old_top2_epg_rate` string COMMENT '老年epgsocretop2', 
  `male_top2_epg_rate` string COMMENT '男性epgsocretop2', 
  `female_top2_epg_rate` string COMMENT '女性epgsocretop2',
  `child_top3_epg_rate` string COMMENT '儿童epgsocretop3', 
  `teen_top3_epg_rate` string COMMENT '青少年epgsocretop3', 
  `youth_top3_epg_rate` string COMMENT '青年epgsocretop3', 
  `mid_top3_epg_rate` string COMMENT '中年epgsocretop3', 
  `old_top3_epg_rate` string COMMENT '老年epgsocretop3', 
  `male_top3_epg_rate` string COMMENT '男性epgsocretop3', 
  `female_top3_epg_rate` string COMMENT '女性epgsocretop3',
  `child_top4_epg_rate` string COMMENT '儿童epgsocretop4', 
  `teen_top4_epg_rate` string COMMENT '青少年epgsocretop4', 
  `youth_top4_epg_rate` string COMMENT '青年epgsocretop4', 
  `mid_top4_epg_rate` string COMMENT '中年epgsocretop4', 
  `old_top4_epg_rate` string COMMENT '老年epgsocretop4', 
  `male_top4_epg_rate` string COMMENT '男性epgsocretop4', 
  `female_top4_epg_rate` string COMMENT '女性epgsocretop4',
  `child_top5_epg_rate` string COMMENT '儿童epgsocretop5', 
  `teen_top5_epg_rate` string COMMENT '青少年epgsocretop5', 
  `youth_top5_epg_rate` string COMMENT '青年epgsocretop5', 
  `mid_top5_epg_rate` string COMMENT '中年epgsocretop5', 
  `old_top5_epg_rate` string COMMENT '老年epgsocretop5', 
  `male_top5_epg_rate` string COMMENT '男性epgsocretop5', 
  `female_top5_epg_rate` string COMMENT '女性epgsocretop5')
COMMENT '用户分时段评分明细表' 
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

  
  
    
      CREATE TABLE knowyou_ott_dmt.`htv_user_rating_dm`( 
  `deviceid` string COMMENT '设备id', 
  `hourtype` string COMMENT '分段时间',  
  `grouptype` string COMMENT '分组',
   `sex_type` string COMMENT '评分')
COMMENT '用户分时段总评分表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期'
  )
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
  
  
  
        CREATE TABLE knowyou_ott_dmt.`htv_user_personas_dm`( 
  `deviceid` string COMMENT '设备id', 
  `hourtype` string COMMENT '分段时间', 
  `rate` string COMMENT '评分', 
  `grouptype` string COMMENT '分组')
COMMENT '用户画像表'
PARTITIONED BY ( 
  `date_time` string COMMENT '日期')
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.mapred.TextInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';