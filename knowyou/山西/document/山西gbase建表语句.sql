CREATE TABLE pdata.hgv_activeadd_retention_mm( 
deal_date varchar(200) COMMENT '日期',
 deviceid varchar(200) COMMENT '设备id', 
 month_activer varchar(200) COMMENT '月活跃用户标签', 
 month_adduer varchar(200) COMMENT '月新增留存用户标签') 
 COMMENT 'B域-同步表-PDATA-用户留存标签月表';
 
 
CREATE TABLE pdata.hgv_activeretentionlabe_dm(
 deal_date varchar(200) COMMENT '日期', 
 deviceid varchar(200) COMMENT '设备id', 
 morrow_activer varchar(200) COMMENT '次日活跃留存用户', 
 seven_activer varchar(200) COMMENT '7日活跃留存用户', 
 fourteen_activer varchar(200) COMMENT '14日活跃留存用户', 
 thirty_activer varchar(200) COMMENT '30日活跃留存用户') 
 COMMENT 'B域-同步表-PDATA-用户活跃留存标签日表';
 
CREATE TABLE pdata.hgv_actorlabel_dm( 
 deal_date varchar(200) COMMENT '日期',
 deviceid varchar(200) COMMENT '设备id', 
 actorname varchar(200) COMMENT '演员名称') 
 COMMENT 'B域-同步表-PDATA-用户收看演员标签标签日表' ;
 
CREATE TABLE pdata.hgv_adduserretentionlabe_dm( 
 deal_date varchar(200) COMMENT '日期',
 deviceid varchar(200) COMMENT '设备id', 
 morrow_activer varchar(200) COMMENT '次日新增留存用户', 
 seven_activer varchar(200) COMMENT '7日新增留存用户', 
 fourteen_activer varchar(200) COMMENT '14日新增留存用户', 
 thirty_activer varchar(200) COMMENT '30日新增留存用户') 
 COMMENT 'B域-同步表-PDATA-用户新增留存标签日表';

CREATE TABLE pdata.hgv_back_flow_lv14( 
 deal_date varchar(200) COMMENT '日期',
 deviceid varchar(200) COMMENT '设备id') 
 COMMENT 'B域-同步表-PDATA-14日回流用户标签表' ;
 
CREATE TABLE pdata.hgv_back_flow_lv30( 
 deal_date varchar(200) COMMENT '日期',
 deviceid varchar(200) COMMENT '设备id') 
 COMMENT 'B域-同步表-PDATA-30日回流用户标签表' ;
 
CREATE TABLE pdata.hgv_back_flow_lv7( 
 deal_date varchar(200) COMMENT '日期',
 deviceid varchar(200) COMMENT '设备id') 
 COMMENT 'B域-同步表-PDATA-7日回流用户标签表' ;
 
CREATE TABLE pdata.hgv_channel_dm( 
 deal_date varchar(200) COMMENT '日期',
 deviceid varchar(200) COMMENT '设备id', 
 channelname varchar(200) COMMENT '直播频道名称', 
 play_duration varchar(200) COMMENT '时长（秒）') 
 COMMENT 'B域-同步表-PDATA-当日直播频道中间表';
 
CREATE TABLE pdata.hgv_channel_prefer_cctv_dm( 
 deal_date varchar(200) COMMENT '日期',
 deviceid varchar(200) COMMENT '设备id', 
 channel_prefer_cctv1 varchar(200) COMMENT '当日cctv1偏好', 
 channel_low_prefer_cctv1 varchar(200) COMMENT '当日cctv1轻偏好', 
 channel_middle_prefer_cctv1 varchar(200) COMMENT '当日cctv1中偏好', 
 channel_high_prefer_cctv1 varchar(200) COMMENT '当日cctv1重偏好', 
 channel_prefer_cctv2 varchar(200) COMMENT '当日cctv2偏好', 
 channel_low_prefer_cctv2 varchar(200) COMMENT '当日cctv2轻偏好', 
 channel_middle_prefer_cctv2 varchar(200) COMMENT '当日cctv2中偏好', 
 channel_high_prefer_cctv2 varchar(200) COMMENT '当日cctv2重偏好', 
 channel_prefer_cctv3 varchar(200) COMMENT '当日cctv3偏好', 
 channel_low_prefer_cctv3 varchar(200) COMMENT '当日cctv3轻偏好', 
 channel_middle_prefer_cctv3 varchar(200) COMMENT '当日cctv3中偏好', 
 channel_high_prefer_cctv3 varchar(200) COMMENT '当日cctv3重偏好', 
 channel_prefer_cctv4 varchar(200) COMMENT '当日cctv4偏好', 
 channel_low_prefer_cctv4 varchar(200) COMMENT '当日cctv4轻偏好', 
 channel_middle_prefer_cctv4 varchar(200) COMMENT '当日cctv4中偏好', 
 channel_high_prefer_cctv4 varchar(200) COMMENT '当日cctv4重偏好', 
 channel_prefer_cctv5 varchar(200) COMMENT '当日cctv5偏好', 
 channel_low_prefer_cctv5 varchar(200) COMMENT '当日cctv5轻偏好', 
 channel_middle_prefer_cctv5 varchar(200) COMMENT '当日cctv5中偏好', 
 channel_high_prefer_cctv5 varchar(200) COMMENT '当日cctv5重偏好', 
 channel_prefer_cctv6 varchar(200) COMMENT '当日cctv6偏好', 
 channel_low_prefer_cctv6 varchar(200) COMMENT '当日cctv6轻偏好', 
 channel_middle_prefer_cctv6 varchar(200) COMMENT '当日cctv6中偏好', 
 channel_high_prefer_cctv6 varchar(200) COMMENT '当日cctv6重偏好', 
 channel_prefer_cctv7 varchar(200) COMMENT '当日cctv7偏好', 
 channel_low_prefer_cctv7 varchar(200) COMMENT '当日cctv7轻偏好', 
 channel_middle_prefer_cctv7 varchar(200) COMMENT '当日cctv7中偏好', 
 channel_high_prefer_cctv7 varchar(200) COMMENT '当日cctv7重偏好', 
 channel_prefer_cctv8 varchar(200) COMMENT '当日cctv8偏好', 
 channel_low_prefer_cctv8 varchar(200) COMMENT '当日cctv8轻偏好', 
 channel_middle_prefer_cctv8 varchar(200) COMMENT '当日cctv8中偏好', 
 channel_high_prefer_cctv8 varchar(200) COMMENT '当日cctv8重偏好', 
 channel_prefer_cctv9 varchar(200) COMMENT '当日cctv9偏好', 
 channel_low_prefer_cctv9 varchar(200) COMMENT '当日cctv9轻偏好', 
 channel_middle_prefer_cctv9 varchar(200) COMMENT '当日cctv9中偏好', 
 channel_high_prefer_cctv9 varchar(200) COMMENT '当日cctv9重偏好', 
 channel_prefer_cctv10 varchar(200) COMMENT '当日cctv10偏好', 
 channel_low_prefer_cctv10 varchar(200) COMMENT '当日cctv10轻偏好', 
 channel_middle_prefer_cctv10 varchar(200) COMMENT '当日cctv10中偏好', 
 channel_high_prefer_cctv10 varchar(200) COMMENT '当日cctv10重偏好', 
 channel_prefer_cctv11 varchar(200) COMMENT '当日cctv11偏好', 
 channel_low_prefer_cctv11 varchar(200) COMMENT '当日cctv11轻偏好', 
 channel_middle_prefer_cctv11 varchar(200) COMMENT '当日cctv11中偏好', 
 channel_high_prefer_cctv11 varchar(200) COMMENT '当日cctv11重偏好', 
 channel_prefer_cctv12 varchar(200) COMMENT '当日cctv12偏好', 
 channel_low_prefer_cctv12 varchar(200) COMMENT '当日cctv12轻偏好', 
 channel_middle_prefer_cctv12 varchar(200) COMMENT '当日cctv12中偏好', 
 channel_high_prefer_cctv12 varchar(200) COMMENT '当日cctv12重偏好', 
 channel_prefer_cctv13 varchar(200) COMMENT '当日cctv13偏好', 
 channel_low_prefer_cctv13 varchar(200) COMMENT '当日cctv13轻偏好', 
 channel_middle_prefer_cctv13 varchar(200) COMMENT '当日cctv13中偏好', 
 channel_high_prefer_cctv13 varchar(200) COMMENT '当日cctv13重偏好', 
 channel_prefer_cctv14 varchar(200) COMMENT '当日cctv14偏好', 
 channel_low_prefer_cctv14 varchar(200) COMMENT '当日cctv14轻偏好', 
 channel_middle_prefer_cctv14 varchar(200) COMMENT '当日cctv14中偏好', 
 channel_high_prefer_cctv14 varchar(200) COMMENT '当日cctv14重偏好', 
 channel_prefer_cctv15 varchar(200) COMMENT '当日cctv15偏好', 
 channel_low_prefer_cctv15 varchar(200) COMMENT '当日cctv15轻偏好', 
 channel_middle_prefer_cctv15 varchar(200) COMMENT '当日cctv15中偏好', 
 channel_high_prefer_cctv15 varchar(200) COMMENT '当日cctv15重偏好')
 COMMENT 'B域-同步表-PDATA-当日直播CCTV频道偏好标签表';
 
 CREATE TABLE pdata.hgv_channel_prefer_dm( 
  deal_date varchar(200) COMMENT '日期',
 deviceid varchar(200) COMMENT '设备id', 
 channel_prefer_lv1 varchar(200) COMMENT '江苏卫视偏好', 
 channel_prefer_lv2 varchar(200) COMMENT '浙江卫视偏好', 
 channel_prefer_lv3 varchar(200) COMMENT '湖南卫视偏好', 
 channel_prefer_lv4 varchar(200) COMMENT '北京卫视偏好', 
 channel_prefer_lv5 varchar(200) COMMENT '深圳卫视偏好', 
 channel_prefer_lv6 varchar(200) COMMENT '辽宁卫视偏好', 
 channel_prefer_lv7 varchar(200) COMMENT '湖北卫视偏好', 
 channel_prefer_lv8 varchar(200) COMMENT '安徽卫视偏好', 
 channel_prefer_lv9 varchar(200) COMMENT '重庆卫视偏好', 
 channel_prefer_lv10 varchar(200) COMMENT '天津卫视偏好', 
 channel_prefer_lv11 varchar(200) COMMENT '黑龙江卫视偏好', 
 channel_prefer_lv12 varchar(200) COMMENT '吉林卫视偏好', 
 channel_prefer_lv13 varchar(200) COMMENT '内蒙古卫视偏好', 
 channel_prefer_lv14 varchar(200) COMMENT '河北卫视偏好', 
 channel_prefer_lv15 varchar(200) COMMENT '河南卫视偏好', 
 channel_prefer_lv16 varchar(200) COMMENT '江西卫视偏好', 
 channel_prefer_lv17 varchar(200) COMMENT '广东卫视偏好', 
 channel_prefer_lv18 varchar(200) COMMENT '广西卫视偏好', 
 channel_prefer_lv19 varchar(200) COMMENT '东南卫视偏好', 
 channel_prefer_lv20 varchar(200) COMMENT '云南卫视偏好', 
 channel_prefer_lv21 varchar(200) COMMENT '贵州卫视偏好', 
 channel_prefer_lv22 varchar(200) COMMENT '山西卫视偏好', 
 channel_prefer_lv23 varchar(200) COMMENT '山东卫视偏好', 
 channel_prefer_lv24 varchar(200) COMMENT '宁夏卫视偏好') 
 COMMENT 'B域-同步表-PDATA-当日直播卫视频道偏好标签表' ;
 
CREATE TABLE pdata.hgv_demand_content_high( 
 deal_date varchar(200) COMMENT '日期',
 deviceid varchar(200) COMMENT '设备id', 
 videoname varchar(200) COMMENT '内容名称') 
 COMMENT 'B域-同步表-PDATA-点播内容100个重偏好标签表';
 
CREATE TABLE pdata.hgv_demand_content_light(
 deal_date varchar(200) COMMENT '日期', 
 deviceid varchar(200) COMMENT '设备id', 
 videoname varchar(200) COMMENT '内容名称') 
 COMMENT 'B域-同步表-PDATA-点播内容100个轻偏好标签表';
 
 
 
 CREATE TABLE pdata.hgv_demand_content_middle( 
  deal_date varchar(200) COMMENT '日期',
 deviceid varchar(200) COMMENT '设备id', 
 videoname varchar(200) COMMENT '内容名称') 
 COMMENT 'B域-同步表-PDATA-点播内容100个中偏好标签表';
 
 
 
CREATE TABLE pdata.hgv_demand_prefer( 
 deal_date varchar(200) COMMENT '日期',
 deviceid varchar(200) COMMENT '设备id', 
 demand_prefer_lv1 varchar(200) COMMENT '当日点播时段0-6点偏好', 
 demand_prefer_lv2 varchar(200) COMMENT '当日点播时段6-9点偏好', 
 demand_prefer_lv3 varchar(200) COMMENT '当日点播时段9-12点偏好', 
 demand_prefer_lv4 varchar(200) COMMENT '当日点播时段12-14点偏好', 
 demand_prefer_lv5 varchar(200) COMMENT '当日点播时段14-18点偏好', 
 demand_prefer_lv6 varchar(200) COMMENT '当日点播时段18-22点偏好', 
 demand_prefer_lv7 varchar(200) COMMENT '当日点播时段22-24点偏好')
 COMMENT 'B域-同步表-PDATA-用户点播时段偏好标签表';
 
CREATE TABLE pdata.hgv_demand_type_prefer_dm(
 deal_date varchar(200) COMMENT '日期', 
 deviceid varchar(200) COMMENT '设备id', 
 demand_type_prefer_lv1 varchar(200) COMMENT '电视剧栏目偏好', 
 demand_type_prefer_lv2 varchar(200) COMMENT '电影栏目偏好', 
 demand_type_prefer_lv3 varchar(200) COMMENT '动漫栏目偏好', 
 demand_type_prefer_lv4 varchar(200) COMMENT '体育栏目偏好', 
 demand_type_prefer_lv5 varchar(200) COMMENT '纪实栏目偏好', 
 demand_type_prefer_lv6 varchar(200) COMMENT '音乐栏目偏好', 
 demand_type_prefer_lv7 varchar(200) COMMENT '新闻栏目偏好', 
 demand_type_prefer_lv8 varchar(200) COMMENT '娱乐栏目偏好', 
 demand_type_prefer_lv9 varchar(200) COMMENT '综艺栏目偏好', 
 demand_type_prefer_lv10 varchar(200) COMMENT '生活栏目偏好', 
 demand_type_prefer_lv11 varchar(200) COMMENT '少儿栏目偏好')
 COMMENT 'B域-同步表-PDATA-当日点播栏目偏好标签表';
 
CREATE TABLE pdata.hgv_directorlabel_dm( 
 deal_date varchar(200) COMMENT '日期',
 deviceid varchar(200) COMMENT '设备id', 
 directorname varchar(200) COMMENT '导演名称') 
 COMMENT 'B域-同步表-PDATA-用户收看导演标签标签日表';
 
CREATE TABLE pdata.hgv_languagelabel_dm( 
 deal_date varchar(200) COMMENT '日期',
 deviceid varchar(200) COMMENT '设备id', 
 v_b1 varchar(200) COMMENT '中文', 
 v_b2 varchar(200) COMMENT '英文', 
 v_b3 varchar(200) COMMENT '法语', 
 v_b4 varchar(200) COMMENT '德语', 
 v_b5 varchar(200) COMMENT '印度语', 
 v_b6 varchar(200) COMMENT '日语', 
 v_b7 varchar(200) COMMENT '韩语', 
 v_b8 varchar(200) COMMENT '泰语', 
 v_b9 varchar(200) COMMENT '预留语言', 
 v_b10 varchar(200) COMMENT '预留语言', 
 v_b11 varchar(200) COMMENT '预留语言', 
 v_b12 varchar(200) COMMENT '预留语言', 
 v_b13 varchar(200) COMMENT '预留语言', 
 v_b14 varchar(200) COMMENT '预留语言', 
 v_b15 varchar(200) COMMENT '预留语言', 
 v_b16 varchar(200) COMMENT '预留语言', 
 v_b17 varchar(200) COMMENT '预留语言', 
 v_b18 varchar(200) COMMENT '预留语言', 
 v_b19 varchar(200) COMMENT '预留语言', 
 v_b20 varchar(200) COMMENT '预留语言') 
 COMMENT 'B域-同步表-PDATA-语言标签表' ;
 
CREATE TABLE pdata.hgv_live_prefer( 
 deal_date varchar(200) COMMENT '日期',
 deviceid varchar(200) COMMENT '设备id', 
 live_prefer_lv1 varchar(200) COMMENT '当日直播时段0-6点偏好', 
 live_prefer_lv2 varchar(200) COMMENT '当日直播时段6-9点偏好', 
 live_prefer_lv3 varchar(200) COMMENT '当日直播时段9-12点偏好', 
 live_prefer_lv4 varchar(200) COMMENT '当日直播时段12-14点偏好', 
 live_prefer_lv5 varchar(200) COMMENT '当日直播时段14-18点偏好', 
 live_prefer_lv6 varchar(200) COMMENT '当日直播时段18-22点偏好', 
 live_prefer_lv7 varchar(200) COMMENT '当日直播时段22-24点偏好')
 COMMENT 'B域-同步表-PDATA-用户直播时段偏好标签表';

 
CREATE TABLE pdata.hgv_plotlabel_dm(
 deal_date varchar(200) COMMENT '日期', 
 deviceid varchar(200) COMMENT '设备id', 
 v_b1 varchar(200) COMMENT '喜剧', 
 v_b2 varchar(200) COMMENT '动作', 
 v_b3 varchar(200) COMMENT '警匪', 
 v_b4 varchar(200) COMMENT '爱情', 
 v_b5 varchar(200) COMMENT '青春', 
 v_b6 varchar(200) COMMENT '犯罪', 
 v_b7 varchar(200) COMMENT '悬疑', 
 v_b8 varchar(200) COMMENT '惊悚', 
 v_b9 varchar(200) COMMENT '恐怖', 
 v_b10 varchar(200) COMMENT '灾难', 
 v_b11 varchar(200) COMMENT '战争', 
 v_b12 varchar(200) COMMENT '科幻', 
 v_b13 varchar(200) COMMENT '武侠', 
 v_b14 varchar(200) COMMENT '纪录片', 
 v_b15 varchar(200) COMMENT '体育', 
 v_b16 varchar(200) COMMENT '幼儿', 
 v_b17 varchar(200) COMMENT '动画', 
 v_b18 varchar(200) COMMENT '励志', 
 v_b19 varchar(200) COMMENT '戏曲', 
 v_b20 varchar(200) COMMENT '广场舞', 
 v_b21 varchar(200) COMMENT '真人秀', 
 v_b22 varchar(200) COMMENT '偶像', 
 v_b23 varchar(200) COMMENT '冒险', 
 v_b24 varchar(200) COMMENT '时尚', 
 v_b25 varchar(200) COMMENT '言情', 
 v_b26 varchar(200) COMMENT '足球', 
 v_b27 varchar(200) COMMENT '篮球', 
 v_b28 varchar(200) COMMENT '网球', 
 v_b29 varchar(200) COMMENT '探索', 
 v_b30 varchar(200) COMMENT '自然', 
 v_b31 varchar(200) COMMENT '人文', 
 v_b32 varchar(200) COMMENT '古装', 
 v_b33 varchar(200) COMMENT '谍战', 
 v_b34 varchar(200) COMMENT '其他') 
 COMMENT 'B域-同步表-PDATA-情节标签表' ;
 
CREATE TABLE pdata.hgv_regionlabel_dm( 
 deal_date varchar(200) COMMENT '日期',
 deviceid varchar(200) COMMENT '设备id', 
 v_b1 varchar(200) COMMENT '内地', 
 v_b2 varchar(200) COMMENT '香港', 
 v_b3 varchar(200) COMMENT '台湾', 
 v_b4 varchar(200) COMMENT '日本', 
 v_b5 varchar(200) COMMENT '韩国', 
 v_b6 varchar(200) COMMENT '泰国', 
 v_b7 varchar(200) COMMENT '美国', 
 v_b8 varchar(200) COMMENT '英国', 
 v_b9 varchar(200) COMMENT '意大利', 
 v_b10 varchar(200) COMMENT '法国', 
 v_b11 varchar(200) COMMENT '俄罗斯', 
 v_b12 varchar(200) COMMENT '德国', 
 v_b13 varchar(200) COMMENT '印度', 
 v_b14 varchar(200) COMMENT '欧洲', 
 v_b15 varchar(200) COMMENT '预留地区标签', 
 v_b16 varchar(200) COMMENT '预留地区标签', 
 v_b17 varchar(200) COMMENT '预留地区标签', 
 v_b18 varchar(200) COMMENT '预留地区标签', 
 v_b19 varchar(200) COMMENT '预留地区标签', 
 v_b20 varchar(200) COMMENT '预留地区标签', 
 v_b21 varchar(200) COMMENT '预留地区标签', 
 v_b22 varchar(200) COMMENT '预留地区标签', 
 v_b23 varchar(200) COMMENT '预留地区标签', 
 v_b24 varchar(200) COMMENT '预留地区标签', 
 v_b25 varchar(200) COMMENT '预留地区标签', 
 v_b26 varchar(200) COMMENT '预留地区标签', 
 v_b27 varchar(200) COMMENT '预留地区标签', 
 v_b28 varchar(200) COMMENT '预留地区标签', 
 v_b29 varchar(200) COMMENT '预留地区标签', 
 v_b30 varchar(200) COMMENT '预留地区标签') 
 COMMENT 'B域-同步表-PDATA-地区标签表' ;
 
CREATE TABLE pdata.hgv_replay_prefer( 
 deal_date varchar(200) COMMENT '日期',
 deviceid varchar(200) COMMENT '设备id', 
 replay_prefer_lv1 varchar(200) COMMENT '当日回看时段0-6点偏好', 
 replay_prefer_lv2 varchar(200) COMMENT '当日回看时段6-9点偏好', 
 replay_prefer_lv3 varchar(200) COMMENT '当日回看时段9-12点偏好', 
 replay_prefer_lv4 varchar(200) COMMENT '当日回看时段12-14点偏好', 
 replay_prefer_lv5 varchar(200) COMMENT '当日回看时段14-18点偏好', 
 replay_prefer_lv6 varchar(200) COMMENT '当日回看时段18-22点偏好', 
 replay_prefer_lv7 varchar(200) COMMENT '当日回看时段22-24点偏好')
 COMMENT 'B域-同步表-PDATA-用户回看时段偏好标签表';
 
CREATE TABLE pdata.hgv_searchcollectlabe_dm( 
 deal_date varchar(200) COMMENT '日期',
 tag varchar(200) COMMENT '标签t1=昨日搜索t2=当月搜索t3=昨日搜索t4=当月收藏', 
 videoname varchar(200) COMMENT '节目名', 
 playnums varchar(200) COMMENT '次数') 
 COMMENT 'B域-同步表-PDATA-用户搜索收藏标签日表';
 
CREATE TABLE pdata.hgv_serv_basic( 
 deal_date varchar(200) COMMENT '日期',
 deviceid varchar(200) COMMENT '设备id', 
 is_boot varchar(200) COMMENT '当日是否开机', 
 is_play varchar(200) COMMENT '当日是否播放', 
 play_duration varchar(200) COMMENT '当日播放时长', 
 play_num varchar(200) COMMENT '当日播放次数', 
 is_live varchar(200) COMMENT '当日收看直播', 
 live_play_num varchar(200) COMMENT '当日收看直播次数', 
 live_play_duration varchar(200) COMMENT '当日收看直播时长', 
 is_demand varchar(200) COMMENT '当日收看点播', 
 demand_play_num varchar(200) COMMENT '当日收看点播次数', 
 demand_play_duration varchar(200) COMMENT '当日收看点播时长', 
 is_replay varchar(200) COMMENT '当日收看回看', 
 replay_play_num varchar(200) COMMENT '当日收看回看次数', 
 replay_play_duration varchar(200) COMMENT '当日收看回看时长')
 COMMENT 'B域-同步表-PDATA-用户活跃信息基础标签表';
 
 CREATE TABLE pdata.hgv_slience_dm( 
  deal_date varchar(200) COMMENT '日期',
 deviceid varchar(200) COMMENT '设备id', 
 pre7_slience_user varchar(200) COMMENT '前7日沉默用户标签', 
 pre14_slience_user varchar(200) COMMENT '前14日沉默用户标签', 
 pre30_slience_user varchar(200) COMMENT '前30日沉默用户标签', 
 pre_month_slience_user varchar(200) COMMENT '上月沉默用户标签')
 COMMENT 'B域-同步表-PDATA-沉默用户标签表' ;
 
CREATE TABLE pdata.hgv_yearslabel_dm( 
 deal_date varchar(200) COMMENT '日期',
 deviceid varchar(200) COMMENT '设备id', 
 v_b1 varchar(200) COMMENT '2020年', 
 v_b2 varchar(200) COMMENT '2019年', 
 v_b3 varchar(200) COMMENT '2018年', 
 v_b4 varchar(200) COMMENT '2017年', 
 v_b5 varchar(200) COMMENT '2010-2016年', 
 v_b6 varchar(200) COMMENT '2000年代', 
 v_b7 varchar(200) COMMENT '90年代', 
 v_b8 varchar(200) COMMENT '80年代', 
 v_b9 varchar(200) COMMENT '70年代', 
 v_b10 varchar(200) COMMENT '预留年代', 
 v_b11 varchar(200) COMMENT '预留年代', 
 v_b12 varchar(200) COMMENT '预留年代', 
 v_b13 varchar(200) COMMENT '预留年代', 
 v_b14 varchar(200) COMMENT '预留年代', 
 v_b15 varchar(200) COMMENT '预留年代', 
 v_b16 varchar(200) COMMENT '预留年代', 
 v_b17 varchar(200) COMMENT '预留年代', 
 v_b18 varchar(200) COMMENT '预留年代', 
 v_b19 varchar(200) COMMENT '预留年代', 
 v_b20 varchar(200) COMMENT '预留年代') 
 COMMENT 'B域-同步表-PDATA-年代标签表' ;



CREATE TABLE "pdata.htv_demand_order_dm" (
  "deal_date" varchar(16) DEFAULT NULL,
  "deviceid" varchar(60) DEFAULT NULL COMMENT '设备id',
  "film_not_order" varchar(20) DEFAULT NULL COMMENT '电影收视用户且未订购',
  "tv_not_order" varchar(20) DEFAULT NULL COMMENT '电视剧收视用户且未订购',
  "child_not_order" varchar(20) DEFAULT NULL COMMENT '少儿收视用户且未订购'
) ENGINE=EXPRESS DEFAULT CHARSET=utf8 TABLESPACE='sys_tablespace' COMMENT='B域-同步表-PDATA-频道分类收视用户未订购标签表'



CREATE TABLE "pdata.htv_film_order_dm" (
  "deal_date" varchar(16) DEFAULT NULL,
  "deviceid" varchar(60) DEFAULT NULL COMMENT '设备id',
  "film_light_not_order" varchar(20) DEFAULT NULL COMMENT '电影点播类别轻偏好且未订购',
  "film_middle_not_order" varchar(20) DEFAULT NULL COMMENT '电影点播类别中偏好且未订购',
  "film_high_not_order" varchar(20) DEFAULT NULL COMMENT '电影点播类别重偏好且未订购'
) ENGINE=EXPRESS DEFAULT CHARSET=utf8 TABLESPACE='sys_tablespace' COMMENT='B域-同步表-PDATA-电影点播类别偏好未订购表'



CREATE TABLE "pdata.htv_tv_order_dm" (
  "deal_date" varchar(16) DEFAULT NULL,
  "deviceid" varchar(60) DEFAULT NULL COMMENT '设备id',
  "tv_light_not_order" varchar(20) DEFAULT NULL COMMENT '电视剧点播类别轻偏好且未订购',
  "tv_middle_not_order" varchar(20) DEFAULT NULL COMMENT '电视剧点播类别中偏好且未订购',
  "tv_high_not_order" varchar(20) DEFAULT NULL COMMENT '电视剧点播类别重偏好且未订购'
) ENGINE=EXPRESS DEFAULT CHARSET=utf8 TABLESPACE='sys_tablespace' COMMENT='B域-同步表-PDATA-电视剧点播类别偏好未订购表'



CREATE TABLE "pdata.htv_child_order_dm" (
  "deal_date" varchar(16) DEFAULT NULL,
  "deviceid" varchar(60) DEFAULT NULL COMMENT '设备id',
  "child_light_not_order" varchar(20) DEFAULT NULL COMMENT '少儿点播类别轻偏好且未订购',
  "child_middle_not_order" varchar(20) DEFAULT NULL COMMENT '少儿点播类别中偏好且未订购',
  "child_high_not_order" varchar(20) DEFAULT NULL COMMENT '少儿点播类别重偏好且未订购'
) ENGINE=EXPRESS DEFAULT CHARSET=utf8 TABLESPACE='sys_tablespace' COMMENT='B域-同步表-PDATA-少儿点播类别偏好未订购表'