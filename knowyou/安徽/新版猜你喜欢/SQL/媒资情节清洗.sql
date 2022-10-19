INSERT OVERWRITE TABLE knowyou_ott_dmt.guess_like_media_plot PARTITION (dt = '${C_DAY}')
SELECT 
  series,
  CASE WHEN plot = '电视台-其他' THEN plot
  WHEN plot like '%电视台-其他|%' THEN REGEXP_REPLACE(plot, '电视台-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|电视台-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '电视台-未知'
      WHEN plot IS NULL THEN '电视台-未知'
      ELSE '电视台-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('电视台')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '电视剧-其他' THEN plot
  WHEN plot like '%电视剧-其他|%' THEN REGEXP_REPLACE(plot, '电视剧-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|电视剧-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '电视剧-未知'
      WHEN plot IS NULL THEN '电视剧-未知'
      WHEN plot IN ('新闻') THEN '电视剧-未知'
      WHEN plot IN ('搞笑', '喜剧', '奇葩', '欢乐', '黑色幽默', '喜悦', '整蛊', '吐槽', '东北喜剧', '娱乐') THEN '电视剧-搞笑'
      WHEN plot IN ('动作', '复仇', '逃亡', '功夫') THEN '电视剧-动作'
      WHEN plot IN ('爱情', '言情', '言情剧', '婚姻', '情感', '情感剧', '亲情', '纯爱', '虐恋', '浪漫', '温情', '家庭爱情', '都市爱情', '悬疑爱情', '偶像都市爱情') THEN '电视剧-情感'
      WHEN plot IN ('网剧') THEN '电视剧-网剧'
      WHEN plot IN ('悬疑','犯罪','警匪剧','悬疑剧','谍战','警匪','刑侦','谍战剧','罪案','推理','缉毒','侦探','惊悚','罪案剧','刑侦剧') THEN '电视剧-悬疑'
      WHEN plot IN ('战斗','战争','军旅','军旅剧','抗战','功夫剧','商战剧','权谋','军事','战争剧','当代军旅','枪战','抗日') THEN '电视剧-战斗'
      WHEN plot IN ('科幻','科幻剧','机战') THEN '电视剧-科幻'
      WHEN plot IN ('动画','少儿','益智') THEN '电视剧-动画'
      WHEN plot IN ('家庭','家族剧','生活','亲子','家族','家庭剧','百科','社会') THEN '电视剧-生活'
      WHEN plot IN ('励志','成长','励志剧') THEN '电视剧-成长'
      WHEN plot IN ('冒险','探索') THEN '电视剧-冒险'
      WHEN plot IN ('奇幻','神话','穿越','魔幻','玄幻','奇幻剧','神话剧','仙侠','魔幻剧') THEN '电视剧-奇幻'
      WHEN plot IN ('传奇','武侠','武侠剧') THEN '电视剧-武侠'
      WHEN plot IN ('时代','年代','历史','年代剧','现代','当代','革命','历史剧','近代','民国','现代剧','历史正剧') THEN '电视剧-时代'
      WHEN plot IN ('电竞','游戏','运动') THEN '电视剧-运动'
      WHEN plot IN ('伦理','伦理剧','人文') THEN '电视剧-伦理'
      WHEN plot IN ('纪实','纪实收费','实录','自然','纪录') THEN '电视剧-纪实'
      WHEN plot IN ('偶像','偶像剧') THEN '电视剧-偶像'
      WHEN plot IN ('青春','校园','小清新') THEN '电视剧-青春'
      WHEN plot IN ('都市','职业剧','职场都市','职场','时尚','都市剧','创业','职业') THEN '电视剧-都市'
      WHEN plot IN ('古装','古装剧','宫廷','明清') THEN '电视剧-古装'
      WHEN plot IN ('农村','乡村','乡村剧') THEN '电视剧-乡村'
      WHEN plot IN ('国产','内地','中文','国语') THEN '电视剧-国产'
      WHEN plot IN ('海外') THEN '电视剧-海外'
      WHEN plot IN ('怀旧', '经典') THEN '电视剧-经典'
      ELSE '电视剧-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('连续剧', '电视剧')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '电影-其他' THEN plot
  WHEN plot like '%电影-其他|%' THEN REGEXP_REPLACE(plot, '电影-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|电影-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '电影-未知'
      WHEN plot IS NULL THEN '电影-未知'
      WHEN plot IN ('新闻') THEN '电影-未知'
      WHEN plot IN ('喜剧','搞笑','奇葩','欢乐','黑色幽默','喜悦','整蛊','吐槽','东北喜剧','娱乐') THEN '电影-搞笑'
      WHEN plot IN ('动作','复仇','逃亡','功夫') THEN '电影-动作'
      WHEN plot IN ('爱情','情感','纯爱','友谊','伤感','爱恋','催泪','三角恋','悲剧','亲情','暧昧','浪漫','治愈','感动','虐恋','恋人','可爱','兄弟关系','禁爱','兄弟情','幸福','回忆','单恋','热恋','暗恋') THEN '电影-情感'
      WHEN plot IN ('惊悚','残酷','恐怖','腹黑','恐惧','震撼','残忍','绝望') THEN '电影-惊悚'
      WHEN plot IN ('悬疑','荒诞','警匪','法治','黑帮','烧脑','警察','罪案','心理','犯罪','大案要案','高智商','推理','卧底','毒品','监狱','阴谋','盗墓','案件','抢劫') THEN '电影-悬疑'
      WHEN plot IN ('战斗','军事','战争','暴力','枪战','燃','杀戮','血腥','杀手','热血','救援','打斗','特工','硬汉','二战','对决','刀剑','暗杀','军人','政治','谍战','史诗','纯爷们','士兵','反击','武器','抗日') THEN '电影-战斗'
      WHEN plot IN ('科幻','超自然','灾难','虚拟时空','超级英雄','脑洞','未来','世界末日','科技','宇宙','外星人','人工智能') THEN '电影-科幻'
      WHEN plot IN ('动画','儿童','动漫','动画电影','童年','哆啦A梦','柯南','中二','最爱动画','奥特曼','蜡笔小新','二次元') THEN '电影-动画'
      WHEN plot IN ('家庭','母亲','婚姻','生活','社会问题','居家','温暖','美食','社会','养生','家庭暴力','旅游','美容','父亲','餐馆','教育','争吵','民生') THEN '电影-生活'
      WHEN plot IN ('成长','励志','人生','奋斗','希望','勇气','忧郁','梦想','世间冷暖','追逐') THEN '电影-成长'
      WHEN plot IN ('冒险','惊险','野外生存','寻宝','探索','调查','夺宝','打怪','历险','探险') THEN '电影-冒险'
      WHEN plot IN ('奇幻','魔幻','玄幻','灵异','穿越','鬼怪','怪兽') THEN '电影-奇幻'
      WHEN plot IN ('仙侠','武侠') THEN '电影-武侠'
      WHEN plot IN ('时代','历史','民国时期','当代','现代','古代','近代（1911-1949）','历史革命','现代（1949-1978）','当代（1978至今）','近代') THEN '电影-时代'
      WHEN plot IN ('体育','运动','电竞','游戏','NBA','汽车','极限运动') THEN '电影-运动'
      WHEN plot IN ('表演','音乐','MV','音乐歌舞','歌舞','秀场','综艺','选秀','舞台','真人秀','文艺','戏曲','流行','华丽','舞台艺术','音乐剧','文化艺术') THEN '电影-表演'
      WHEN plot IN ('纪录','自然','纪实','实录','纪实收费','资料','纪录片','动物','森林','法律','律师','海洋') THEN '电影-纪录'
      WHEN plot IN ('伦理','道德','人性','人文','宗教') THEN '电影-伦理'
      WHEN plot IN ('青春','清新','校园','青少年','少女','青年','教师') THEN '电影-青春'
      WHEN plot IN ('都市','街道','公路','美女','18岁以上','女权','职场','时尚','时尚生活') THEN '电影-都市'
      WHEN plot IN ('古装','古装历史') THEN '电影-古装'
      WHEN plot IN ('4K','1080P','杜比音效','高清','超清1080P') THEN '电影-画质'
      WHEN plot IN ('国产','普通话','华语','内地','香港地区','粤语','香港影史经典','台湾地区') THEN '电影-国产'
      WHEN plot IN ('海外','美国','欧美','英语','欧美','丹麦','英国','欧洲','法国','日本','日语','法语','加拿大','西部','德国','俄罗斯','印度','意大利','澳大利亚','瑞典') THEN '电影-海外'
      WHEN plot IN ('经典','怀旧','老电影') THEN '电影-经典'
      WHEN plot IN ('乡村','农村','乡村') THEN '电影-乡村'
      WHEN plot IN ('奥斯卡佳片','排行榜','百度风云榜','豆瓣高分榜','影史TOP100导演','豆瓣高分','口碑佳片','华语票房TOP','全球票房TOP1000','豆瓣TOP250','金马奖佳片') THEN '电影-榜单'
      ELSE '电影-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('4K', '电影')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '少儿-其他' THEN plot
  WHEN plot like '%少儿-其他|%' THEN REGEXP_REPLACE(plot, '少儿-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|少儿-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '少儿-未知'
      WHEN plot IS NULL THEN '少儿-未知'
      WHEN plot IN ('新闻') THEN '少儿-未知'
      WHEN plot IN ('儿歌','音乐','启蒙儿歌','乐器','诗词','国学儿歌','儿歌KTV') THEN '少儿-儿歌'
      WHEN plot IN ('益智','益智玩具') THEN '少儿-益智'
      WHEN plot IN ('手工绘画','手工','绘本','绘画','美术','DIY') THEN '少儿-手工绘画'
      WHEN plot IN ('玩具','玩具故事') THEN '少儿-玩具'
      WHEN plot IN ('英语','单词拼读','英文儿歌','英语启蒙','英文动画') THEN '少儿-英语'
      WHEN plot IN ('早教','课程','百科','素质教育','拼音识字','亲子教育','幼教','常识','启蒙','阅读','交通知识','消防知识','育儿') THEN '少儿-早教'
      WHEN plot IN ('数学') THEN '少儿-数学'
      WHEN plot IN ('国学') THEN '少儿-国学'
      WHEN plot IN ('故事','冒险','奇幻','魔幻','探险','神话','搞笑','动画','热血','迪士尼','科幻','机战','战斗','魔法','故事','童话','战争','卡通','格斗','历史') THEN '少儿-故事'
      WHEN plot IN ('能力','励志','认知能力','口才','想象力','团队协作','成长','情商培养','思维训练','习惯养成','艺术启蒙','语言能力','安全知识','解决问题','认知','舞蹈','地理','科普','探索科普','正义','竞技') THEN '少儿-能力'
      WHEN plot IN ('动植物','萌宠','动物','宠物','植物','自然') THEN '少儿-动植物'
      WHEN plot IN ('真人特摄','真人') THEN '少儿-真人特摄'
      WHEN plot IN ('校园','保卫家园','青春','生活','环保','运动','家庭','友情','救援','亲子','趣味','互动游戏','合家欢','车','公益') THEN '少儿-生活'
      WHEN plot IN ('男孩','男孩喜欢','王子') THEN '少儿-男孩'
      WHEN plot IN ('女孩','公主','女孩喜欢') THEN '少儿-女孩'
      WHEN plot IN ('幼儿','0-3岁','3岁','2岁','1岁') THEN '少儿-0至3岁'
      WHEN plot IN ('4-6岁','6岁','5岁','4岁') THEN '少儿-4至6岁'
      WHEN plot IN ('7-10岁','7岁','8岁','9岁','10岁','一年级','二年级','7岁以上','三年级') THEN '少儿-7至10岁'
      WHEN plot IN ('10岁以上','11-13岁','11岁','12岁','13岁','14岁','15岁','16岁','17岁','四年级','五年级','六年级') THEN '少儿-10岁以上'
      ELSE '少儿-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('0', '熊猫', '少儿')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '综艺-其他' THEN plot
  WHEN plot like '%综艺-其他|%' THEN REGEXP_REPLACE(plot, '综艺-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|综艺-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '综艺-未知'
      WHEN plot IS NULL THEN '综艺-未知'
      WHEN plot IN ('新闻') THEN '综艺-未知'
      WHEN plot IN ('竞技','游戏') THEN '综艺-游戏'
      WHEN plot IN ('音乐','说唱','音乐现场') THEN '综艺-音乐'
      WHEN plot IN ('亲情','婚姻','情感') THEN '综艺-情感'
      WHEN plot IN ('搞笑','娱乐','喜剧') THEN '综艺-喜剧'
      WHEN plot IN ('真人秀','明星','脱口秀','选秀','春晚','明星类','央视春晚') THEN '综艺-真人秀'
      WHEN plot IN ('表演','舞蹈','竞演','歌舞') THEN '综艺-表演'
      WHEN plot IN ('亲子','教育','益智','少儿') THEN '综艺-亲子'
      WHEN plot IN ('文化','文艺','曲艺','人文历史','文学艺术','人文') THEN '综艺-文化'
      WHEN plot IN ('美食','生活','旅游','相亲','健康','装修','旅行','家庭伦理') THEN '综艺-生活'
      WHEN plot IN ('职场') THEN '综艺-职场'
      WHEN plot IN ('潮流','时尚','八卦','娱乐新闻','精编','美妆') THEN '综艺-潮流'
      WHEN plot IN ('卫视热播','央视','北京卫视','浙江卫视','东方卫视','湖南卫视','江苏卫视') THEN '综艺-卫视'
      WHEN plot IN ('萌宠','宠物') THEN '综艺-萌宠'
      WHEN plot IN ('访谈','播报','咨询') THEN '综艺-访谈'
      WHEN plot IN ('晚会','歌舞晚会') THEN '综艺-晚会'
      WHEN plot IN ('小品','相声小品') THEN '综艺-小品'
      WHEN plot IN ('网络平台','自制','芒果出品','网络综艺','爱奇艺出品') THEN '综艺-网络平台'
      ELSE '综艺-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('旅游', '综艺')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '音乐-其他' THEN plot
  WHEN plot like '%音乐-其他|%' THEN REGEXP_REPLACE(plot, '音乐-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|音乐-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '音乐-未知'
      WHEN plot IS NULL THEN '音乐-未知'
      WHEN plot IN ('新闻') THEN '音乐-未知'
      WHEN plot IN ('MV','专辑') THEN '音乐-MV'
      WHEN plot IN ('演唱会','LIVE现场','现场','音乐会') THEN '音乐-演唱会'
      WHEN plot IN ('排行榜','歌友会','影视金曲','集锦','栏目','合集') THEN '音乐-栏目'
      WHEN plot IN ('内地') THEN '音乐-内地' 
      WHEN plot IN ('日韩') THEN '音乐-日韩'
      WHEN plot IN ('欧美') THEN '音乐-欧美'
      WHEN plot IN ('流行') THEN '音乐-流行'
      WHEN plot IN ('摇滚') THEN '音乐-摇滚'
      WHEN plot IN ('R&B') THEN '音乐-R&B'
      WHEN plot IN ('电子') THEN '音乐-电子'
      WHEN plot IN ('爵士') THEN '音乐-爵士'
      WHEN plot IN ('说唱','Hip-Hop/说唱') THEN '音乐-说唱'
      WHEN plot IN ('民族','曲艺','革命军旅','民族音乐','民谣') THEN '音乐-民族'
      WHEN plot IN ('古典') THEN '音乐-古典'
      WHEN plot IN ('嘻哈') THEN '音乐-嘻哈'
      WHEN plot IN ('舞曲','歌舞','晚会') THEN '音乐-舞曲'
      ELSE '音乐-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('音乐')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '体育-其他' THEN plot
  WHEN plot like '%体育-其他|%' THEN REGEXP_REPLACE(plot, '体育-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|体育-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '体育-未知'
      WHEN plot IS NULL THEN '体育-未知'
      WHEN plot IN ('新闻') THEN '体育-未知'
      WHEN plot IN ('常规赛','精彩得分','精彩配合','精彩助攻','精彩扣篮','NBA','精彩抢断','精彩三分球','精彩篮板','CBA','中国职业篮球联赛CBA','男篮','NBA常规赛','精彩盖帽','空接','季后赛','辽宁本钢','CBA季后赛','篮球','技术犯规','高得分','高助攻','篮板','G2') THEN '体育-篮球'
      WHEN plot IN ('精彩进球','角球','头球','精彩扑救','远射','妙传','任意球','越位','黄牌','点球','法甲','意甲','西甲','英超','足球','铲球','法甲诸强','西甲诸强','意甲诸强','德甲','本国联赛','体育赛事','世界波','VAR','红牌','英超诸强','世界杯','德甲诸强','团队配合','中国男足') THEN '体育-足球'
      WHEN plot IN ('冬奥会','冬残奥会','自由式滑雪','滑雪','冰壶','花样滑冰','中国队','女子项目','男子项目','世界诸强','金牌','夺冠','U型场地技巧','障碍追逐','高山滑雪','冰球','速度滑冰','雪车','2022北京冬奥会','单板滑雪') THEN '体育-冬奥会'
      WHEN plot IN ('2022WTT','男子单打','女子单打','WTT常规挑战赛','球星','男子双打','女子双打','大满贯','混合双打','正赛','WTT','WTT球星挑战赛','新加坡站','乒乓球','多哈站资格赛') THEN '体育-乒乓球'
      WHEN plot IN ('羽毛球世锦赛','羽毛球','汤尤杯羽毛球赛','苏迪曼杯') THEN '体育-羽毛球'
      WHEN plot IN ('田径','跳高','跳远','三级跳远','铅球','标枪','跑步','竞走','国际田联','接力跑') THEN '体育-田径'
      WHEN plot IN ('台球','斯诺克','斯诺克世锦赛','斯诺克欧洲大师赛','李照','斯诺克威尔士公开赛','世锦赛') THEN '体育-台球'
      WHEN plot IN ('电竞','游戏','手机游戏','主播解说','沙盒游戏','哒啵电竞','PC游戏','单机游戏','像素游戏','网易游戏','我的世界','主机游戏','MOBA','生存游戏','冒险游戏') THEN '体育-电竞'
      WHEN plot IN ('汽车','赛车') THEN '体育-赛车'  
      WHEN plot IN ('UFC','UFC格斗之夜','UFC272','KO','格斗','UFC273') THEN '体育-格斗'
      WHEN plot IN ('网球','中国网球巡回赛') THEN '体育-网球'
      ELSE '体育-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('运动', '体育')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '教育-其他' THEN plot
  WHEN plot like '%教育-其他|%' THEN REGEXP_REPLACE(plot, '教育-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|教育-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '教育-未知'
      WHEN plot IS NULL THEN '教育-未知'
      WHEN plot IN ('新闻') THEN '教育-未知'
      WHEN plot IN ('公开课','课程','公开课','语文','数学','历史','英语','地理','政治','物理','化学','生物','综合语文','科学') THEN '教育-公开课'
      WHEN plot IN ('小学') THEN '教育-小学'
      WHEN plot IN ('中学','初中','初一','初三') THEN '教育-中学'
      WHEN plot IN ('高中') THEN '教育-高中'
      WHEN plot IN ('大学') THEN '教育-大学'
      WHEN plot IN ('人文历史','文化','传统文化','国学','人文','诗词') THEN '教育-国学'
      WHEN plot IN ('早教','儿歌','动画','逗你学','诸葛学堂','熊猫博士','益智','可爱巧虎岛','语言','语言学习') THEN '教育-早教'
      WHEN plot IN ('普法','案件','法律') THEN '教育-法律'
      WHEN plot IN ('职业教育','职场','创业') THEN '教育-职业教育'
      WHEN plot IN ('素质教育','科普','科教','健康','情感','百科','艺术','音乐','体育','美术','技能','科学科普','兴趣爱好') THEN '教育-素质教育'
      WHEN plot IN ('在线','安徽在线教育','安徽20年6月教育','收费教育','新东方kids') THEN '教育-在线教育'
      ELSE '教育-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('法治', '知识', '教育')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '片花-其他' THEN plot
  WHEN plot like '%片花-其他|%' THEN REGEXP_REPLACE(plot, '片花-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|片花-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '片花-未知'
      WHEN plot IS NULL THEN '片花-未知'
      WHEN plot IN ('新闻') THEN '片花-未知'
      WHEN plot IN ('电影') THEN '片花-电影'
      WHEN plot IN ('电视剧') THEN '片花-电视剧'
      ELSE '片花-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('片花')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '纪录片-其他' THEN plot
  WHEN plot like '%纪录片-其他|%' THEN REGEXP_REPLACE(plot, '纪录片-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|纪录片-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '纪录片-未知'
      WHEN plot IS NULL THEN '纪录片-未知'
      WHEN plot IN ('新闻') THEN '纪录片-未知'
      WHEN plot IN ('历史') THEN '纪录片-历史'
      WHEN plot IN ('军事','战争','军旅','现代战争') THEN '纪录片-军事'
      WHEN plot IN ('社会','社会问题','人物','传记','政治') THEN '纪录片-社会'
      WHEN plot IN ('自然','动物','灾难','风景') THEN '纪录片-自然'
      WHEN plot IN ('人文','文化','艺术','传统文化','人文历史','宗教') THEN '纪录片-人文'
      WHEN plot IN ('犯罪','刑侦') THEN '纪录片-罪案'
      WHEN plot IN ('科学','科技') THEN '纪录片-科技'
      WHEN plot IN ('美食') THEN '纪录片-美食'
      WHEN plot IN ('旅游') THEN '纪录片-旅游'
      WHEN plot IN ('地理','考古') THEN '纪录片-地理'
      WHEN plot IN ('探索','探秘') THEN '纪录片-探索'
      ELSE '纪录片-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('纪录片')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '娱乐-其他' THEN plot
  WHEN plot like '%娱乐-其他|%' THEN REGEXP_REPLACE(plot, '娱乐-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|娱乐-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '娱乐-未知'
      WHEN plot IS NULL THEN '娱乐-未知'
      WHEN plot IN ('新闻') THEN '娱乐-未知'
      WHEN plot IN ('咨询','专题') THEN '娱乐-咨询'
      WHEN plot IN ('综艺','真人秀','脱口秀','选秀','舞台剧','生活') THEN '娱乐-综艺'
      WHEN plot IN ('喜剧','小品','搞笑','娱乐','相声','喜剧盛典') THEN '娱乐-喜剧'
      WHEN plot IN ('比赛') THEN '娱乐-比赛'
      WHEN plot IN ('文化','曲艺','秦腔','越剧','黄梅戏','沪剧','京剧','淮剧') THEN '娱乐-文化'
      WHEN plot IN ('音乐','音乐会','我们的歌','MV','爱乐之都') THEN '娱乐-音乐'
      WHEN plot IN ('晚会','颁奖礼') THEN '娱乐-晚会'
      WHEN plot IN ('访谈','专访') THEN '娱乐-访谈'
      WHEN plot IN ('情感') THEN '娱乐-情感'
      WHEN plot IN ('游戏') THEN '娱乐-游戏'
      WHEN plot IN ('萌宠') THEN '娱乐-萌宠'
      ELSE '娱乐-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('娱乐')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '时尚-其他' THEN plot
  WHEN plot like '%时尚-其他|%' THEN REGEXP_REPLACE(plot, '时尚-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|时尚-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '时尚-未知'
      WHEN plot IS NULL THEN '时尚-未知'
      WHEN plot IN ('新闻') THEN '时尚-未知'    
      ELSE '时尚-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('时尚')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '搞笑-其他' THEN plot
  WHEN plot like '%搞笑-其他|%' THEN REGEXP_REPLACE(plot, '搞笑-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|搞笑-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '搞笑-未知'
      WHEN plot IS NULL THEN '搞笑-未知'
      WHEN plot IN ('新闻') THEN '搞笑-未知'
      ELSE '搞笑-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('搞笑')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '游戏-其他' THEN plot
  WHEN plot like '%游戏-其他|%' THEN REGEXP_REPLACE(plot, '游戏-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|游戏-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '游戏-未知'
      WHEN plot IS NULL THEN '游戏-未知'
      WHEN plot IN ('新闻') THEN '游戏-未知'
      ELSE '游戏-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('游戏')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '新闻-其他' THEN plot
  WHEN plot like '%新闻-其他|%' THEN REGEXP_REPLACE(plot, '新闻-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|新闻-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '新闻-未知'
      WHEN plot IS NULL THEN '新闻-未知'
      WHEN plot IN ('资讯','时政','时评') THEN '新闻-时政'
      WHEN plot IN ('社会','国际','军事','经济','科技','民生') THEN '新闻-社会'
      WHEN plot IN ('收藏','历史','美食') THEN '新闻-奇趣'
      WHEN plot IN ('人物','人文') THEN '新闻-人物'
      WHEN plot IN ('财经','第一财经','金融','理财') THEN '新闻-财经'
      WHEN plot IN ('CCTV2','东方卫视','CCTV-新闻标清','CCTV-4标清') THEN '新闻-卫视'
      ELSE '新闻-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('财经', '资讯', '新闻')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '生活-其他' THEN plot
  WHEN plot like '%生活-其他|%' THEN REGEXP_REPLACE(plot, '生活-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|生活-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '生活-未知'
      WHEN plot IS NULL THEN '生活-未知'
      WHEN plot IN ('新闻') THEN '生活-未知'
      WHEN plot IN ('健康','养生','中医养生','健康之路') THEN '生活-健康'
      WHEN plot IN ('美食','消化篇') THEN '生活-美食'
      WHEN plot IN ('休闲','炫舞未来','曲艺','秀场','京剧','名段','搞笑','歌舞','戏曲','音乐','集锦','春晚','才艺','太极','达人','走秀','综艺','美容','时尚') THEN '生活-休闲'
      WHEN plot IN ('情感') THEN '生活-情感'
      WHEN plot IN ('百科','常识','食药篇','肾功能','健康知识课堂','五官篇','三农','医学','交流分享','金色学堂','人口（2019）','科教') THEN '生活-窍门'
      WHEN plot IN ('居家','购物','家居') THEN '生活-居家'
      WHEN plot IN ('宠物', '萌宠') THEN '生活-萌宠'
      WHEN plot IN ('动作','舞蹈','健身','广场舞','体育','瑜伽','健身操','体育健身') THEN '生活-运动'
      WHEN plot IN ('钓鱼','旅游','旅行户外','猎奇') THEN '生活-户外'
      WHEN plot IN ('少儿','母婴','动漫','儿童舞蹈') THEN '生活-少儿'
      ELSE '生活-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('高清', 'DIY', '健身', '养生', '动作', '戏曲', '时尚生活', '瑜伽', '职业', '艺术', '健康', '太极', '孕育', '家居', '曲艺', '武术', '母婴', '美颜', '美食', '舞蹈', '生活')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '动漫-其他' THEN plot
  WHEN plot like '%动漫-其他|%' THEN REGEXP_REPLACE(plot, '动漫-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|动漫-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '动漫-未知'
      WHEN plot IS NULL THEN '动漫-未知'
      WHEN plot IN ('新闻') THEN '动漫-未知'
      WHEN plot IN ('普通话','中国大陆','武侠','江湖','宫廷','狐妖小红娘','玄仙','玄幻','神魔','神怪','仙侠') THEN '动漫-国漫'
      WHEN plot IN ('日本','日语','ACG') THEN '动漫-日漫'
      WHEN plot IN ('古代','历史','怀旧','现代','近代','历史革命','经典','近代（1911-1949）','现代（1949-1978）','贵族') THEN '动漫-时代'
      WHEN plot IN ('治愈','催泪') THEN '动漫-治愈'
      WHEN plot IN ('热血','格斗','战斗','竞技','机战','体育竞技','动作','运动','战争','复仇','武士','英雄','超级英雄（美式）','战士/士兵','忍者','牛仔','英雄人物') THEN '动漫-战斗'
      WHEN plot IN ('奇幻','魔幻','魔法','神话','超能力者','穿越','神话人物','骑士','魔法师/女巫','精灵','魔术师','幽灵（西方）','魔法少女','幻界王') THEN '动漫-奇幻'
      WHEN plot IN ('科幻','未来','架空','特摄','宇宙','海洋','外星人','机器人','蝙蝠侠','末日','想象力') THEN '动漫-科幻'
      WHEN plot IN ('推理','悬疑','侦探','斗智','惊悚','名侦探柯南','犯罪','恐怖') THEN '动漫-悬疑'
      WHEN plot IN ('冒险','冒险者','山林','雨林','草原','湿地','历险','冻原','西部','野外') THEN '动漫-冒险'
      WHEN plot IN ('搞笑','趣味','娱乐') THEN '动漫-搞笑'
      WHEN plot IN ('后宫') THEN '动漫-后宫'
      WHEN plot IN ('恋爱','御姐','萝莉','言情','CP','爱情') THEN '动漫-恋爱'
      WHEN plot IN ('青春','校园','励志','初高中','成长','少女向') THEN '动漫-青春'
      WHEN plot IN ('日常','偶像','合家欢','萌宠','职场','社会','美食','小镇','城市','音乐','宠物','人类','乡村','动物（会说人话）','生活','讽刺','常识','百科','和风','安全知识','商人','都市','音乐家') THEN '动漫-日常'
      WHEN plot IN ('原创','漫改','游戏改','轻改','漫画改编','游戏改编','轻小说改编','改编','2D','3D','短片','泡面','特别篇','动画电影','英语','欧美','韩语','韩国','真人') THEN '动漫-动画类型'
      WHEN plot IN ('亲子','0-3岁','早教') THEN '动漫-0至3岁'
      WHEN plot IN ('4-6岁','幼教','益智','童话') THEN '动漫-4至6岁'
      WHEN plot IN ('7-10岁','小学生','11-13岁','动物') THEN '动漫-7至13岁'
      WHEN plot IN ('14-17岁','少年向','教育') THEN '动漫-14至17岁'
      WHEN plot IN ('18以上','18岁以上','青年向') THEN '动漫-18以上'
      ELSE '动漫-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('动漫')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '其他-其他' THEN plot
  WHEN plot like '%其他-其他|%' THEN REGEXP_REPLACE(plot, '其他-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|其他-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '其他-未知'
      WHEN plot IS NULL THEN '其他-未知'
      WHEN plot IN ('新闻') THEN '其他-未知'
      WHEN plot IN ('财经','经济','国内','时政','百科','医学科普','资讯') THEN '其他-知识'
      WHEN plot IN ('生活''食品''健康''居家''母婴''服饰''珠宝''广场舞''收藏''家居''社会''情感''健身''美食''旅游''养生''节日''健身操''美容用品') THEN '其他-生活'
      WHEN plot IN ('央视名栏','热点解读','评书','评剧','少儿','秀场','春晚','炫舞未来','益智','历史','人物写真','大法官开庭','特别节目','选秀','央视','时尚','音乐会','宣传片') THEN '其他-栏目'
      WHEN plot IN ('京剧','戏曲','豫剧','歌舞','晚会','越剧','秦腔','相声','黄梅戏','琼剧','小品','川剧','昆曲','人文','粤剧','曲艺','未来有戏','舞蹈','搞笑','河北梆子','沪剧','淮剧','晋剧','文化','锡剧','高甲戏','花鼓戏','二人转','花灯剧','苏剧','吕剧','彝剧') THEN '其他-民族艺术'
      WHEN plot IN ('数码','汽车2','电器','汽车','车展','科技') THEN '其他-科技'
      ELSE '其他-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('栏目', '汽车', '汽车2', '休闲', '央视名栏', '文化', '科技', '短视频', '其他')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '测试-其他' THEN plot
  WHEN plot like '%测试-其他|%' THEN REGEXP_REPLACE(plot, '测试-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|测试-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '测试-未知'
      WHEN plot IS NULL THEN '测试-未知'
      WHEN plot IN ('新闻') THEN '测试-未知'   
      ELSE '测试-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('itvyc002', 'itvyc003', 'itvyc004', 'itvyc005')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '党建-其他' THEN plot
  WHEN plot like '%党建-其他|%' THEN REGEXP_REPLACE(plot, '党建-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|党建-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '党建-未知'
      WHEN plot IS NULL THEN '党建-未知'
      WHEN plot IN ('新闻') THEN '党建-未知'
      WHEN plot IN ('专题片','纪录片','专题片/纪录片','记录片') THEN '党建-专题'
      WHEN plot IN ('社会','民生','教育','疫情','常识','法律','健康知识课堂') THEN '党建-生活'
      WHEN plot IN ('军事') THEN '党建-军事'
      WHEN plot IN ('经济') THEN '党建-经济'
      WHEN plot IN ('科技') THEN '党建-科技'
      ELSE '党建-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('政论片', '党建影视', '党建时政', '党建资讯')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '电竞-其他' THEN plot
  WHEN plot like '%电竞-其他|%' THEN REGEXP_REPLACE(plot, '电竞-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|电竞-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '电竞-未知'
      WHEN plot IS NULL THEN '电竞-未知'
      WHEN plot IN ('新闻') THEN '电竞-未知'
      WHEN plot IN ('MOBA','英雄联盟','MOBA游戏','300英雄','起凡三国','dota') THEN '电竞-MOBA'
      WHEN plot IN ('绝地求生','CSGO','守望先锋') THEN '电竞-FPS'
      WHEN plot IN ('沙盒游戏','我的世界','迷你世界','方块世界','沙盒世界') THEN '电竞-沙盒'
      WHEN plot IN ('手机游戏','王者荣耀','和平精英','手游酷玩','猫和老鼠') THEN '电竞-手游'
      WHEN plot IN ('腾讯游戏','网络游戏','网易游戏') THEN '电竞-网游'
      WHEN plot IN ('单机游戏','主机游戏','像素游戏','生存游戏','单机主机','STEAM','植物大战僵尸','单机热游') THEN '电竞-单机'
      WHEN plot IN ('主播解说','哒啵电竞','游戏攻略','女神解说','张大仙','英雄教学') THEN '电竞-解说'
      WHEN plot IN ('精彩集锦','搞笑集锦','大神操作') THEN '电竞-集锦'
      WHEN plot IN ('职业选手','热门赛事','2021KPL秋季赛','KPL','2021KPL春季赛','LPL') THEN '电竞-选手赛事'
      ELSE '电竞-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('电子竞技', '电竞')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '购物-其他' THEN plot
  WHEN plot like '%购物-其他|%' THEN REGEXP_REPLACE(plot, '购物-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|购物-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '购物-未知'
      WHEN plot IS NULL THEN '购物-未知'
      WHEN plot IN ('新闻') THEN '购物-未知'
      ELSE '购物-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('购物')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '亲子-其他' THEN plot
  WHEN plot like '%亲子-其他|%' THEN REGEXP_REPLACE(plot, '亲子-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|亲子-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '亲子-未知'
      WHEN plot IS NULL THEN '亲子-未知'
      WHEN plot IN ('新闻') THEN '亲子-未知'     
      ELSE '亲子-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('亲子')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b

UNION ALL

SELECT 
  series,
  CASE WHEN plot = '纪实-其他' THEN plot
  WHEN plot like '%纪实-其他|%' THEN REGEXP_REPLACE(plot, '纪实-其他\\|', '') 
  ELSE REGEXP_REPLACE(plot, '\\|纪实-其他', '')END plot
FROM
(
  SELECT 
    series, 
    CONCAT_WS('|', COLLECT_SET(plot)) plot
  FROM
  (
    SELECT
      series, 
      CASE WHEN plot IN ('NULL', '空', '未知', '不详', 'None', '无', '') THEN '纪实-未知'
      WHEN plot IS NULL THEN '纪实-未知'
      WHEN plot IN ('新闻') THEN '纪实-未知'
      WHEN plot IN ('历史','资料') THEN '纪实-历史'
      WHEN plot IN ('人文','文化','艺术') THEN '纪实-人文'
      WHEN plot IN ('自然','动物') THEN '纪实-自然'
      WHEN plot IN ('军事','二战','抗战') THEN '纪实-军事'
      WHEN plot IN ('探索','探险','探索发现','地理','Discovery','探秘','猎奇','宇宙') THEN '纪实-探索'
      WHEN plot IN ('社会','人物','传记','犯罪') THEN '纪实-社会'
      WHEN plot IN ('科技','科普') THEN '纪实-科技'
      WHEN plot IN ('美食','生活','旅游','真人秀','宠物') THEN '纪实-生活'
      ELSE '纪实-其他' END plot
    FROM knowyou_ott_ods.dim_pub_video_df
    LATERAL VIEW EXPLODE(split(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(series_keywords, ' ', ''), ',', '|'), '，', '|'), '\\|')) col AS plot
    WHERE dt = '${C_DAY}' and series_type IN ('纪实')
    GROUP BY series, plot
  ) a
  GROUP BY series
) b
;