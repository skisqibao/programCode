/*
Navicat MySQL Data Transfer

Source Server         : 10.186.78.40
Source Server Version : 50730
Source Host           : 10.186.78.40:13306
Source Database       : ky_intelligent_new

Target Server Type    : MYSQL
Target Server Version : 50730
File Encoding         : 65001

Date: 2022-08-30 17:18:46
*/

SET FOREIGN_KEY_CHECKS=0;

-- ----------------------------
-- Table structure for groupcount
-- ----------------------------
DROP TABLE IF EXISTS `groupcount`;
CREATE TABLE `groupcount` (
  `group_id` int(11) DEFAULT NULL,
  `user_num` int(11) DEFAULT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for ky_license_plate
-- ----------------------------
DROP TABLE IF EXISTS `ky_license_plate`;
CREATE TABLE `ky_license_plate` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `cp_license` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '牌照license',
  `cp_code` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '牌照编码',
  `cp_name` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '牌照名称',
  `sort` int(10) DEFAULT NULL COMMENT '排序',
  `remark` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '备注',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建日期',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=41 DEFAULT CHARSET=utf8 COLLATE=utf8_bin ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for ky_role_permission
-- ----------------------------
DROP TABLE IF EXISTS `ky_role_permission`;
CREATE TABLE `ky_role_permission` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `role_id` int(11) NOT NULL COMMENT '角色id',
  `permission_id` int(11) NOT NULL COMMENT '权限id',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=22 DEFAULT CHARSET=utf8 COLLATE=utf8_bin ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for ky_role_plate
-- ----------------------------
DROP TABLE IF EXISTS `ky_role_plate`;
CREATE TABLE `ky_role_plate` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `role_id` int(11) NOT NULL COMMENT '角色id',
  `licenseplate_id` int(11) NOT NULL COMMENT '牌照id',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=6 DEFAULT CHARSET=utf8 COLLATE=utf8_bin ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for ky_roles_menu
-- ----------------------------
DROP TABLE IF EXISTS `ky_roles_menu`;
CREATE TABLE `ky_roles_menu` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `menu_id` int(11) NOT NULL COMMENT '菜单id',
  `role_id` int(11) NOT NULL COMMENT '角色id',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for ky_sys_menu
-- ----------------------------
DROP TABLE IF EXISTS `ky_sys_menu`;
CREATE TABLE `ky_sys_menu` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `name` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '菜单名称',
  `pid` int(10) DEFAULT NULL COMMENT '上级菜单ID',
  `alias` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT '别名',
  `url` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '链接地址',
  `sort` int(10) DEFAULT NULL COMMENT '排序',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for ky_sys_role
-- ----------------------------
DROP TABLE IF EXISTS `ky_sys_role`;
CREATE TABLE `ky_sys_role` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `datascope` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT 'dataScope',
  `level` int(2) DEFAULT NULL COMMENT '层级',
  `name` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '角色名称',
  `remark` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '备注',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建日期',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=9 DEFAULT CHARSET=utf8 COLLATE=utf8_bin ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for ky_sys_user
-- ----------------------------
DROP TABLE IF EXISTS `ky_sys_user`;
CREATE TABLE `ky_sys_user` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `email` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT '邮箱',
  `enabled` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT '状态：1启用、0禁用',
  `password` varchar(200) COLLATE utf8_bin DEFAULT NULL COMMENT '密码',
  `phone` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT '手机号',
  `username` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '用户名',
  `province` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT '省份',
  `city` varchar(50) COLLATE utf8_bin DEFAULT NULL COMMENT '城市',
  `area` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '区域',
  `create_time` timestamp NULL DEFAULT NULL COMMENT '创建日期',
  `lastpass_wordreset_time` timestamp NULL DEFAULT NULL COMMENT '最后修改密码的日期',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=52 DEFAULT CHARSET=utf8 COLLATE=utf8_bin ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for ky_user_role
-- ----------------------------
DROP TABLE IF EXISTS `ky_user_role`;
CREATE TABLE `ky_user_role` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `user_id` int(11) NOT NULL COMMENT '用户id',
  `role_id` int(11) NOT NULL COMMENT '角色id',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=28 DEFAULT CHARSET=utf8 COLLATE=utf8_bin ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for permisson
-- ----------------------------
DROP TABLE IF EXISTS `permisson`;
CREATE TABLE `permisson` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `name` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '名称',
  `pid` int(10) DEFAULT NULL COMMENT '上级菜单ID',
  `alias` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '别名',
  `url` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '链接地址',
  `sort` int(10) DEFAULT NULL COMMENT '排序',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建日期',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=29 DEFAULT CHARSET=utf8 COLLATE=utf8_bin ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for portrait_usermanagement
-- ----------------------------
DROP TABLE IF EXISTS `portrait_usermanagement`;
CREATE TABLE `portrait_usermanagement` (
  `id` int(22) NOT NULL AUTO_INCREMENT,
  `deviceid` varchar(255) DEFAULT NULL,
  `userid` varchar(255) DEFAULT NULL,
  `province` varchar(50) DEFAULT NULL,
  `city` varchar(50) DEFAULT NULL,
  `liscense` varchar(20) DEFAULT NULL,
  `day_id` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  KEY `a` (`deviceid`) USING BTREE,
  KEY `b` (`userid`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=317135 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for rec_group_category_hot
-- ----------------------------
DROP TABLE IF EXISTS `rec_group_category_hot`;
CREATE TABLE `rec_group_category_hot` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` bigint(20) DEFAULT NULL COMMENT '分组序号1,2,3,4,5',
  `video_id` varchar(255) DEFAULT NULL,
  `category` varchar(255) DEFAULT NULL COMMENT '类别（电影、电视剧、少儿....）',
  `rating` bigint(255) DEFAULT NULL COMMENT '推荐度（热度），排序依据',
  `license` varchar(255) DEFAULT NULL,
  `date_time` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=974218 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for rec_group_category_order
-- ----------------------------
DROP TABLE IF EXISTS `rec_group_category_order`;
CREATE TABLE `rec_group_category_order` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` bigint(20) DEFAULT NULL,
  `category` varchar(255) DEFAULT NULL COMMENT '类别（电影、电视剧、少儿）',
  `watchcount` bigint(255) DEFAULT NULL COMMENT '总观看次数，排序依据',
  `license` varchar(255) DEFAULT NULL,
  `date_time` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=21082 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for rec_group_guess
-- ----------------------------
DROP TABLE IF EXISTS `rec_group_guess`;
CREATE TABLE `rec_group_guess` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` bigint(20) DEFAULT NULL COMMENT '分组序号1,2,3,4,5',
  `video_id` varchar(255) DEFAULT NULL,
  `rating` varchar(255) DEFAULT NULL COMMENT '推荐度，排序依据',
  `license` varchar(255) DEFAULT NULL,
  `date_time` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=56352 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for rec_group_label
-- ----------------------------
DROP TABLE IF EXISTS `rec_group_label`;
CREATE TABLE `rec_group_label` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` bigint(20) DEFAULT NULL COMMENT '分组序号1,2,3,4,5',
  `avg_watchcount` varchar(255) DEFAULT NULL COMMENT '平均月总观看次数',
  `avg_watchtime` bigint(20) DEFAULT NULL COMMENT '平均月总观看时间',
  `date_time` varchar(255) DEFAULT NULL,
  `license` varchar(25) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=2345 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for rec_group_user_list
-- ----------------------------
DROP TABLE IF EXISTS `rec_group_user_list`;
CREATE TABLE `rec_group_user_list` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `group_id` bigint(20) DEFAULT NULL,
  `device_id` varchar(255) DEFAULT NULL COMMENT '分组序号1,2,3,4,5',
  `license` varchar(255) DEFAULT NULL,
  `date_time` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  KEY `group_id` (`group_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=82509717 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for sun_temp
-- ----------------------------
DROP TABLE IF EXISTS `sun_temp`;
CREATE TABLE `sun_temp` (
  `actor_id` longtext COLLATE utf8mb4_bin,
  `actor_name` longtext COLLATE utf8mb4_bin,
  `actor_english_name` longtext COLLATE utf8mb4_bin,
  `birthday` longtext COLLATE utf8mb4_bin,
  `birthplace` longtext COLLATE utf8mb4_bin,
  `bloodtype` longtext COLLATE utf8mb4_bin,
  `nation` longtext COLLATE utf8mb4_bin,
  `sex` longtext COLLATE utf8mb4_bin,
  `constellation` longtext COLLATE utf8mb4_bin,
  `country` longtext COLLATE utf8mb4_bin,
  `graduate_school` longtext COLLATE utf8mb4_bin,
  `height` longtext COLLATE utf8mb4_bin,
  `profession` longtext COLLATE utf8mb4_bin,
  `urlpicture` longtext COLLATE utf8mb4_bin,
  `works` longtext COLLATE utf8mb4_bin,
  `brief_introduction` longtext CHARACTER SET utf8 COMMENT '简介',
  `poster_path` longtext COLLATE utf8mb4_bin,
  `status` longtext COLLATE utf8mb4_bin,
  `license` longtext COLLATE utf8mb4_bin,
  `tag` longtext COLLATE utf8mb4_bin,
  `create_time` longtext COLLATE utf8mb4_bin
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for sys_area
-- ----------------------------
DROP TABLE IF EXISTS `sys_area`;
CREATE TABLE `sys_area` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `code` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT '编码',
  `name` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT '名称',
  `region_code` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT '区域编码',
  `province_code` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT '省份编码',
  `city_code` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT '城市编码',
  `alias` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT '别名',
  `area_level` int(2) DEFAULT NULL COMMENT '区域层级',
  `sort` int(10) DEFAULT NULL COMMENT '排序',
  `parent_id` int(10) DEFAULT NULL COMMENT '上级id',
  `status` int(2) DEFAULT NULL COMMENT '状态',
  `create_by` int(10) DEFAULT NULL COMMENT '创建者',
  `update_by` int(10) DEFAULT NULL COMMENT '修改者',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  `update_time` timestamp NOT NULL DEFAULT '1970-01-01 10:00:00' COMMENT '修改时间',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for t_ht_deviceidinfo
-- ----------------------------
DROP TABLE IF EXISTS `t_ht_deviceidinfo`;
CREATE TABLE `t_ht_deviceidinfo` (
  `id` bigint(21) NOT NULL AUTO_INCREMENT,
  `device_id` varchar(255) DEFAULT NULL COMMENT '设备信息的id',
  `province` varchar(255) DEFAULT NULL COMMENT '省份的编码',
  `city` varchar(255) DEFAULT NULL COMMENT '城市的编码',
  `create_time` varchar(255) DEFAULT NULL COMMENT '设备产生的时间',
  `user_id` varchar(255) DEFAULT NULL COMMENT '设备产生的时间',
  `apk_version` varchar(255) DEFAULT NULL,
  `terminal_mode` varchar(255) DEFAULT NULL,
  `terminal_id` varchar(255) DEFAULT NULL,
  `probesoft_version` varchar(255) DEFAULT NULL,
  `supplier_name` varchar(255) DEFAULT NULL,
  `connect_mode` varchar(255) DEFAULT NULL,
  `license` varchar(255) DEFAULT NULL,
  `date_time` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `DEVICEIDCITY` (`device_id`,`city`,`create_time`,`probesoft_version`),
  KEY `CITY` (`city`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=765746146 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_mz_actorinfo
-- ----------------------------
DROP TABLE IF EXISTS `t_mz_actorinfo`;
CREATE TABLE `t_mz_actorinfo` (
  `id` bigint(11) NOT NULL AUTO_INCREMENT,
  `actor_id` varchar(255) DEFAULT NULL,
  `actor_name` varchar(255) DEFAULT NULL,
  `actor_english_name` varchar(255) DEFAULT NULL COMMENT '别名',
  `birthday` varchar(255) DEFAULT NULL,
  `birthplace` varchar(255) DEFAULT NULL,
  `bloodtype` varchar(255) DEFAULT NULL COMMENT '血型',
  `nation` varchar(255) DEFAULT NULL COMMENT '民族',
  `sex` varchar(255) DEFAULT NULL,
  `constellation` varchar(255) DEFAULT NULL COMMENT '星座',
  `country` varchar(255) DEFAULT NULL,
  `graduate_school` varchar(255) DEFAULT NULL COMMENT '毕业学校',
  `height` varchar(255) DEFAULT NULL,
  `profession` varchar(255) DEFAULT NULL COMMENT '职业',
  `urlpicture` varchar(255) DEFAULT NULL COMMENT '探针图片url',
  `works` text,
  `brief_introduction` varchar(255) CHARACTER SET utf8mb4 DEFAULT NULL COMMENT '简介',
  `poster_path` varchar(255) DEFAULT NULL COMMENT '图片路径',
  `status` varchar(255) DEFAULT NULL COMMENT '1：新增需填充 2：以填充图片 3：人工维护',
  `license` varchar(255) CHARACTER SET gbk DEFAULT NULL COMMENT '牌照方',
  `tag` varchar(255) DEFAULT NULL COMMENT '标签：预留字段',
  `create_time` varchar(255) DEFAULT NULL,
  `update_time` datetime DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE,
  KEY `actor_id` (`actor_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=22544017 DEFAULT CHARSET=utf8;

-- ----------------------------
-- Table structure for t_mz_actorinfo_copy
-- ----------------------------
DROP TABLE IF EXISTS `t_mz_actorinfo_copy`;
CREATE TABLE `t_mz_actorinfo_copy` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `actor_id` varchar(255) DEFAULT NULL,
  `actor_name` varchar(255) DEFAULT NULL,
  `actor_english_name` varchar(255) DEFAULT NULL COMMENT '别名',
  `birthday` varchar(255) DEFAULT NULL,
  `birthplace` varchar(255) DEFAULT NULL,
  `bloodtype` varchar(255) DEFAULT NULL COMMENT '血型',
  `nation` varchar(255) DEFAULT NULL COMMENT '民族',
  `sex` varchar(255) DEFAULT NULL,
  `constellation` varchar(255) DEFAULT NULL COMMENT '星座',
  `country` varchar(255) DEFAULT NULL,
  `graduate_school` varchar(255) DEFAULT NULL COMMENT '毕业学校',
  `height` varchar(255) DEFAULT NULL,
  `profession` varchar(255) DEFAULT NULL COMMENT '职业',
  `urlpicture` varchar(255) DEFAULT NULL COMMENT '探针图片url',
  `works` text,
  `brief_introduction` varchar(255) DEFAULT NULL COMMENT '简介',
  `poster_path` varchar(255) DEFAULT NULL COMMENT '图片路径',
  `status` varchar(255) NOT NULL COMMENT '1：新增需填充 2：以填充图片 3：人工维护',
  `license` varchar(255) DEFAULT NULL COMMENT '牌照方',
  `tag` varchar(255) DEFAULT NULL COMMENT '标签：预留字段',
  `create_time` datetime DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  `update_time` datetime DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=33563 DEFAULT CHARSET=utf8mb4;

-- ----------------------------
-- Table structure for t_mz_filmbasicinfo
-- ----------------------------
DROP TABLE IF EXISTS `t_mz_filmbasicinfo`;
CREATE TABLE `t_mz_filmbasicinfo` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `video_id` varchar(255) DEFAULT NULL,
  `video_name` varchar(255) DEFAULT NULL,
  `video_time_length` varchar(255) DEFAULT NULL COMMENT '节目时长',
  `video_type` varchar(255) DEFAULT NULL COMMENT '分类：电影，电视剧，综艺，少儿等',
  `video_plot` text COMMENT '类型：悬疑|惊悚等',
  `video_region` varchar(255) DEFAULT NULL COMMENT '地区',
  `direct_name` varchar(255) DEFAULT NULL,
  `actor_name` text,
  `language` varchar(255) DEFAULT NULL,
  `release_date` varchar(255) DEFAULT NULL COMMENT '发行年份',
  `hImg` varchar(255) DEFAULT NULL COMMENT '图片地址',
  `vImg` varchar(255) DEFAULT NULL COMMENT '海报地址',
  `information` text COMMENT '影片详情',
  `play_counts` varchar(255) DEFAULT NULL COMMENT '页面显示点播次数',
  `poster_path` varchar(255) DEFAULT NULL COMMENT '图片路径',
  `status` varchar(255) DEFAULT NULL COMMENT '0：新增需填充 1：以填充图片',
  `license` varchar(255) DEFAULT NULL COMMENT '牌照方',
  `tag` varchar(255) DEFAULT NULL COMMENT '标签：预留字段',
  `create_time` datetime DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  `update_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  KEY `video_id` (`video_id`) USING BTREE,
  KEY `sdf` (`video_id`,`license`) USING BTREE,
  KEY `license` (`license`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=225280 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for t_mz_filmbasicinfo_copy
-- ----------------------------
DROP TABLE IF EXISTS `t_mz_filmbasicinfo_copy`;
CREATE TABLE `t_mz_filmbasicinfo_copy` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `video_id` varchar(255) DEFAULT NULL,
  `video_name` varchar(255) DEFAULT NULL,
  `video_time_length` varchar(255) DEFAULT NULL COMMENT '节目时长',
  `video_type` varchar(255) DEFAULT NULL COMMENT '分类：电影，电视剧，综艺，少儿等',
  `video_plot` text COMMENT '类型：悬疑|惊悚等',
  `video_region` varchar(255) DEFAULT NULL COMMENT '地区',
  `direct_name` varchar(255) DEFAULT NULL,
  `actor_name` text,
  `language` varchar(255) DEFAULT NULL,
  `release_date` varchar(255) DEFAULT NULL COMMENT '发行年份',
  `hImg` varchar(255) DEFAULT NULL COMMENT '图片地址',
  `vImg` varchar(255) DEFAULT NULL COMMENT '海报地址',
  `information` text COMMENT '影片详情',
  `play_counts` varchar(255) DEFAULT NULL COMMENT '页面显示点播次数',
  `poster_path` varchar(255) DEFAULT NULL COMMENT '图片路径',
  `status` varchar(255) DEFAULT NULL COMMENT '0：新增需填充 1：以填充图片',
  `license` varchar(255) DEFAULT NULL COMMENT '牌照方',
  `tag` varchar(255) DEFAULT NULL COMMENT '标签：预留字段',
  `create_time` datetime DEFAULT NULL ON UPDATE CURRENT_TIMESTAMP,
  `update_time` datetime DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  KEY `video_id` (`video_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=139118 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for t_yq_filmerinfo
-- ----------------------------
DROP TABLE IF EXISTS `t_yq_filmerinfo`;
CREATE TABLE `t_yq_filmerinfo` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `video_id` varchar(255) DEFAULT NULL,
  `video_name` varchar(255) DEFAULT NULL,
  `secondlevel` varchar(255) DEFAULT NULL COMMENT '栏目',
  `playnums` bigint(20) DEFAULT NULL COMMENT '访问次数',
  `license` varchar(255) DEFAULT NULL COMMENT '牌照方',
  `day_id` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  KEY `video_id` (`video_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=32752119 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for t_yq_hotactortop
-- ----------------------------
DROP TABLE IF EXISTS `t_yq_hotactortop`;
CREATE TABLE `t_yq_hotactortop` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `actor_id` varchar(50) DEFAULT NULL,
  `actor_name` varchar(255) DEFAULT NULL,
  `playnums` bigint(255) DEFAULT NULL,
  `license` varchar(255) DEFAULT NULL,
  `day_id` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`id`) USING BTREE,
  KEY `actor_id` (`actor_id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=20139 DEFAULT CHARSET=utf8 ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for visits
-- ----------------------------
DROP TABLE IF EXISTS `visits`;
CREATE TABLE `visits` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `ip_address` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT 'ip地址',
  `user` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '用户',
  `create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB AUTO_INCREMENT=5 DEFAULT CHARSET=utf8 COLLATE=utf8_bin ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- Table structure for visits_you
-- ----------------------------
DROP TABLE IF EXISTS `visits_you`;
CREATE TABLE `visits_you` (
  `id` int(11) NOT NULL AUTO_INCREMENT COMMENT '主键id',
  `ipaddress` varchar(20) COLLATE utf8_bin DEFAULT NULL COMMENT 'ip地址',
  `user` varchar(100) COLLATE utf8_bin DEFAULT NULL COMMENT '用户',
  `createtime` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '创建时间',
  PRIMARY KEY (`id`) USING BTREE
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin ROW_FORMAT=DYNAMIC;

-- ----------------------------
-- View structure for v_common_filmbasicinfo
-- ----------------------------
DROP VIEW IF EXISTS `v_common_filmbasicinfo`;
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v_common_filmbasicinfo` AS select `t_mz_filmbasicinfo`.`id` AS `id`,`t_mz_filmbasicinfo`.`video_id` AS `video_id`,`t_mz_filmbasicinfo`.`video_name` AS `video_name`,`t_mz_filmbasicinfo`.`video_time_length` AS `video_time_length`,`t_mz_filmbasicinfo`.`video_type` AS `video_type`,`t_mz_filmbasicinfo`.`video_plot` AS `video_plot`,(case when (`t_mz_filmbasicinfo`.`video_region` like '%中国%') then '中国大陆' when (`t_mz_filmbasicinfo`.`video_region` like '%内地%') then '中国大陆' when (`t_mz_filmbasicinfo`.`video_region` like '%台湾%') then '中国台湾' when (`t_mz_filmbasicinfo`.`video_region` like '%日本%') then '日本' when (`t_mz_filmbasicinfo`.`video_region` like '%法国%') then '法国' when (`t_mz_filmbasicinfo`.`video_region` like '%德国%') then '德国' when (`t_mz_filmbasicinfo`.`video_region` like '%意大利%') then '意大利' when (`t_mz_filmbasicinfo`.`video_region` like '%印度%') then '印度' when (`t_mz_filmbasicinfo`.`video_region` like '%泰国%') then '泰国' else '其他' end) AS `video_region`,`t_mz_filmbasicinfo`.`direct_name` AS `direct_name`,`t_mz_filmbasicinfo`.`actor_name` AS `actor_name`,`t_mz_filmbasicinfo`.`language` AS `language`,(case when (`t_mz_filmbasicinfo`.`release_date` like '%200%') then '00年代' when (`t_mz_filmbasicinfo`.`release_date` like '%199%') then '90年代' when (`t_mz_filmbasicinfo`.`release_date` like '%198%') then '其他' when (`t_mz_filmbasicinfo`.`release_date` like '%197%') then '其他' when (`t_mz_filmbasicinfo`.`release_date` like '%196%') then '其他' when (`t_mz_filmbasicinfo`.`release_date` like '%195%') then '其他' else `t_mz_filmbasicinfo`.`release_date` end) AS `release_date`,`t_mz_filmbasicinfo`.`hImg` AS `hImg`,`t_mz_filmbasicinfo`.`vImg` AS `vImg`,`t_mz_filmbasicinfo`.`information` AS `information`,`t_mz_filmbasicinfo`.`play_counts` AS `play_counts`,`t_mz_filmbasicinfo`.`poster_path` AS `poster_path`,`t_mz_filmbasicinfo`.`status` AS `status`,`t_mz_filmbasicinfo`.`license` AS `license`,`t_mz_filmbasicinfo`.`tag` AS `tag` from `t_mz_filmbasicinfo` order by `t_mz_filmbasicinfo`.`play_counts` desc ;

-- ----------------------------
-- View structure for v_filmbasicinfo
-- ----------------------------
DROP VIEW IF EXISTS `v_filmbasicinfo`;
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v_filmbasicinfo` AS select `ky_intelligent_new`.`t_mz_filmbasicinfo`.`id` AS `id`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_id` AS `video_id`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_name` AS `video_name`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_time_length` AS `video_time_length`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_type` AS `video_type`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_plot` AS `video_plot`,(case when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_region` like '%中国%') then '中国大陆' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_region` like '%内地%') then '中国大陆' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_region` like '%台湾%') then '中国台湾' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_region` like '%日本%') then '日本' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_region` like '%法国%') then '法国' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_region` like '%德国%') then '德国' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_region` like '%意大利%') then '意大利' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_region` like '%印度%') then '印度' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_region` like '%泰国%') then '泰国' else '其他' end) AS `video_region`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`direct_name` AS `direct_name`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`actor_name` AS `actor_name`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`language` AS `language`,(case when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`release_date` like '%200%') then '00年代' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`release_date` like '%199%') then '90年代' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`release_date` like '%198%') then '其他' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`release_date` like '%197%') then '其他' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`release_date` like '%196%') then '其他' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`release_date` like '%195%') then '其他' else `ky_intelligent_new`.`t_mz_filmbasicinfo`.`release_date` end) AS `release_date`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`hImg` AS `hImg`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`vImg` AS `vImg`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`information` AS `information`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`play_counts` AS `play_counts`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`poster_path` AS `poster_path`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`status` AS `status`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`license` AS `license`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`tag` AS `tag`,`t_yq_filmerinfo`.`secondlevel` AS `secondlevel`,`t_yq_filmerinfo`.`playnums` AS `playnums`,`t_yq_filmerinfo`.`day_id` AS `dayId` from (((select `ky_intelligent_new`.`t_yq_filmerinfo`.`id` AS `id`,`ky_intelligent_new`.`t_yq_filmerinfo`.`video_id` AS `video_id`,`ky_intelligent_new`.`t_yq_filmerinfo`.`video_name` AS `video_name`,`ky_intelligent_new`.`t_yq_filmerinfo`.`secondlevel` AS `secondlevel`,`ky_intelligent_new`.`t_yq_filmerinfo`.`playnums` AS `playnums`,`ky_intelligent_new`.`t_yq_filmerinfo`.`license` AS `license`,`ky_intelligent_new`.`t_yq_filmerinfo`.`day_id` AS `day_id` from `ky_intelligent_new`.`t_yq_filmerinfo` where `ky_intelligent_new`.`t_yq_filmerinfo`.`day_id` in (select max(`ky_intelligent_new`.`t_yq_filmerinfo`.`day_id`) from `ky_intelligent_new`.`t_yq_filmerinfo`))) `t_yq_filmerinfo` left join `ky_intelligent_new`.`t_mz_filmbasicinfo` on((`t_yq_filmerinfo`.`video_id` = `ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_id`))) where (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`id` is not null) order by `ky_intelligent_new`.`t_mz_filmbasicinfo`.`play_counts` desc ;

-- ----------------------------
-- View structure for v_filmbasicinfo_selected
-- ----------------------------
DROP VIEW IF EXISTS `v_filmbasicinfo_selected`;
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v_filmbasicinfo_selected` AS select `ky_intelligent_new`.`t_mz_filmbasicinfo`.`id` AS `id`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_id` AS `video_id`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_name` AS `video_name`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_time_length` AS `video_time_length`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_type` AS `video_type`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_plot` AS `video_plot`,(case when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_region` like '%中国%') then '中国大陆' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_region` like '%内地%') then '中国大陆' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_region` like '%台湾%') then '中国台湾' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_region` like '%日本%') then '日本' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_region` like '%法国%') then '法国' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_region` like '%德国%') then '德国' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_region` like '%意大利%') then '意大利' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_region` like '%印度%') then '印度' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_region` like '%泰国%') then '泰国' else '其他' end) AS `video_region`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`direct_name` AS `direct_name`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`actor_name` AS `actor_name`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`language` AS `language`,(case when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`release_date` like '%200%') then '00年代' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`release_date` like '%199%') then '90年代' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`release_date` like '%198%') then '其他' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`release_date` like '%197%') then '其他' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`release_date` like '%196%') then '其他' when (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`release_date` like '%195%') then '其他' else `ky_intelligent_new`.`t_mz_filmbasicinfo`.`release_date` end) AS `release_date`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`hImg` AS `hImg`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`vImg` AS `vImg`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`information` AS `information`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`play_counts` AS `play_counts`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`poster_path` AS `poster_path`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`status` AS `status`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`license` AS `license`,`ky_intelligent_new`.`t_mz_filmbasicinfo`.`tag` AS `tag`,`t_yq_filmerinfo`.`secondlevel` AS `secondlevel`,`t_yq_filmerinfo`.`playnums` AS `playnums`,`t_yq_filmerinfo`.`day_id` AS `dayId` from (((select `ky_intelligent_new`.`t_yq_filmerinfo`.`id` AS `id`,`ky_intelligent_new`.`t_yq_filmerinfo`.`video_id` AS `video_id`,`ky_intelligent_new`.`t_yq_filmerinfo`.`video_name` AS `video_name`,`ky_intelligent_new`.`t_yq_filmerinfo`.`secondlevel` AS `secondlevel`,`ky_intelligent_new`.`t_yq_filmerinfo`.`playnums` AS `playnums`,`ky_intelligent_new`.`t_yq_filmerinfo`.`license` AS `license`,`ky_intelligent_new`.`t_yq_filmerinfo`.`day_id` AS `day_id` from `ky_intelligent_new`.`t_yq_filmerinfo` where `ky_intelligent_new`.`t_yq_filmerinfo`.`day_id` in (select max(`ky_intelligent_new`.`t_yq_filmerinfo`.`day_id`) from `ky_intelligent_new`.`t_yq_filmerinfo`))) `t_yq_filmerinfo` left join `ky_intelligent_new`.`t_mz_filmbasicinfo` on((`t_yq_filmerinfo`.`video_id` = `ky_intelligent_new`.`t_mz_filmbasicinfo`.`video_id`))) where (`ky_intelligent_new`.`t_mz_filmbasicinfo`.`id` is not null) ;

-- ----------------------------
-- View structure for v_group_user_list
-- ----------------------------
DROP VIEW IF EXISTS `v_group_user_list`;
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v_group_user_list` AS select `rec_group_user_list`.`id` AS `id`,`rec_group_user_list`.`group_id` AS `group_id`,`rec_group_user_list`.`device_id` AS `device_id`,`rec_group_user_list`.`license` AS `license`,`rec_group_user_list`.`date_time` AS `date_time` from `rec_group_user_list` limit 20 ;

-- ----------------------------
-- View structure for v_ky_license_plate
-- ----------------------------
DROP VIEW IF EXISTS `v_ky_license_plate`;
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v_ky_license_plate` AS select `ky_license_plate`.`id` AS `id`,`ky_license_plate`.`cp_code` AS `cp_code`,`ky_license_plate`.`cp_name` AS `cp_name`,`ky_license_plate`.`sort` AS `sort`,`ky_license_plate`.`remark` AS `remark`,`ky_license_plate`.`create_time` AS `create_time`,`ky_role_permission`.`role_id` AS `roleId` from (`ky_license_plate` left join `ky_role_permission` on((`ky_role_permission`.`permission_id` = `ky_license_plate`.`id`))) ;

-- ----------------------------
-- View structure for v_ky_sys_role
-- ----------------------------
DROP VIEW IF EXISTS `v_ky_sys_role`;
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v_ky_sys_role` AS select `ky_sys_role`.`id` AS `id`,`ky_sys_role`.`datascope` AS `datascope`,`ky_sys_role`.`level` AS `level`,`ky_sys_role`.`name` AS `name`,`ky_sys_role`.`remark` AS `remark`,`ky_sys_role`.`create_time` AS `create_time`,group_concat(`ky_role_plate`.`licenseplate_id` separator ',') AS `plateIds`,group_concat(`ky_role_permission`.`permission_id` separator ',') AS `permissionIds` from ((`ky_sys_role` left join `ky_role_plate` on((`ky_sys_role`.`id` = `ky_role_plate`.`role_id`))) left join `ky_role_permission` on((`ky_sys_role`.`id` = `ky_role_permission`.`role_id`))) group by `ky_sys_role`.`id` ;

-- ----------------------------
-- View structure for v_ky_sys_user
-- ----------------------------
DROP VIEW IF EXISTS `v_ky_sys_user`;
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v_ky_sys_user` AS select `ky_sys_user`.`id` AS `id`,`ky_sys_user`.`email` AS `email`,`ky_sys_user`.`enabled` AS `enabled`,`ky_sys_user`.`password` AS `password`,`ky_sys_user`.`phone` AS `phone`,`ky_sys_user`.`username` AS `username`,`ky_sys_user`.`province` AS `province`,`ky_sys_user`.`city` AS `city`,`ky_sys_user`.`area` AS `area`,`ky_sys_user`.`create_time` AS `create_time`,`ky_sys_user`.`lastpass_wordreset_time` AS `lastpasswordresettime`,group_concat(`ky_sys_role`.`name` separator ',') AS `roleName`,group_concat(`ky_user_role`.`role_id` separator ',') AS `roleIds` from ((`ky_sys_user` left join `ky_user_role` on((`ky_sys_user`.`id` = `ky_user_role`.`user_id`))) left join `ky_sys_role` on((`ky_user_role`.`role_id` = `ky_sys_role`.`id`))) group by `ky_sys_user`.`id` ;

-- ----------------------------
-- View structure for v_permisson
-- ----------------------------
DROP VIEW IF EXISTS `v_permisson`;
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v_permisson` AS select `permisson`.`id` AS `id`,`permisson`.`pid` AS `pid`,`permisson`.`name` AS `name`,`permisson`.`alias` AS `alias`,`permisson`.`url` AS `url`,`permisson`.`sort` AS `sort` from `permisson` where (`permisson`.`pid` = 0) union all select distinct `pnb`.`id` AS `id`,`pnb`.`pid` AS `pid`,`pnb`.`name` AS `name`,`pnb`.`alias` AS `alias`,`pnb`.`url` AS `url`,`pnb`.`sort` AS `sort` from (`permisson` `pna` left join `permisson` `pnb` on((`pna`.`id` = `pnb`.`pid`))) ;

-- ----------------------------
-- View structure for v_rec_group_guess
-- ----------------------------
DROP VIEW IF EXISTS `v_rec_group_guess`;
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v_rec_group_guess` AS select `t_mz_filmbasicinfo`.`id` AS `id`,`t_mz_filmbasicinfo`.`video_id` AS `video_id`,`t_mz_filmbasicinfo`.`video_name` AS `video_name`,`t_mz_filmbasicinfo`.`video_time_length` AS `video_time_length`,`t_mz_filmbasicinfo`.`video_type` AS `video_type`,`t_mz_filmbasicinfo`.`video_plot` AS `video_plot`,(case when (`t_mz_filmbasicinfo`.`video_region` like '%中国%') then '中国大陆' when (`t_mz_filmbasicinfo`.`video_region` like '%内地%') then '中国大陆' when (`t_mz_filmbasicinfo`.`video_region` like '%台湾%') then '中国台湾' when (`t_mz_filmbasicinfo`.`video_region` like '%日本%') then '日本' when (`t_mz_filmbasicinfo`.`video_region` like '%法国%') then '法国' when (`t_mz_filmbasicinfo`.`video_region` like '%德国%') then '德国' when (`t_mz_filmbasicinfo`.`video_region` like '%意大利%') then '意大利' when (`t_mz_filmbasicinfo`.`video_region` like '%印度%') then '印度' when (`t_mz_filmbasicinfo`.`video_region` like '%泰国%') then '泰国' else '其他' end) AS `video_region`,`t_mz_filmbasicinfo`.`direct_name` AS `direct_name`,`t_mz_filmbasicinfo`.`actor_name` AS `actor_name`,`t_mz_filmbasicinfo`.`language` AS `language`,(case when (`t_mz_filmbasicinfo`.`release_date` like '%200%') then '00年代' when (`t_mz_filmbasicinfo`.`release_date` like '%199%') then '90年代' when (`t_mz_filmbasicinfo`.`release_date` like '%198%') then '其他' when (`t_mz_filmbasicinfo`.`release_date` like '%197%') then '其他' when (`t_mz_filmbasicinfo`.`release_date` like '%196%') then '其他' when (`t_mz_filmbasicinfo`.`release_date` like '%195%') then '其他' else `t_mz_filmbasicinfo`.`release_date` end) AS `release_date`,`t_mz_filmbasicinfo`.`hImg` AS `hImg`,`t_mz_filmbasicinfo`.`vImg` AS `vImg`,`t_mz_filmbasicinfo`.`information` AS `information`,`t_mz_filmbasicinfo`.`play_counts` AS `play_counts`,`t_mz_filmbasicinfo`.`poster_path` AS `poster_path`,`t_mz_filmbasicinfo`.`status` AS `status`,`t_mz_filmbasicinfo`.`license` AS `license`,`t_mz_filmbasicinfo`.`tag` AS `tag`,`rec_group_guess`.`group_id` AS `groupId`,`rec_group_guess`.`rating` AS `groupRating`,`rec_group_guess`.`license` AS `groupLicense` from (`rec_group_guess` left join `t_mz_filmbasicinfo` on((`rec_group_guess`.`video_id` = `t_mz_filmbasicinfo`.`video_id`))) ;

-- ----------------------------
-- View structure for v_rec_group_hot
-- ----------------------------
DROP VIEW IF EXISTS `v_rec_group_hot`;
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v_rec_group_hot` AS select `t_mz_filmbasicinfo`.`id` AS `id`,`t_mz_filmbasicinfo`.`video_id` AS `video_id`,`t_mz_filmbasicinfo`.`video_name` AS `video_name`,`t_mz_filmbasicinfo`.`video_time_length` AS `video_time_length`,`t_mz_filmbasicinfo`.`video_type` AS `video_type`,`t_mz_filmbasicinfo`.`video_plot` AS `video_plot`,`t_mz_filmbasicinfo`.`video_region` AS `video_region`,`t_mz_filmbasicinfo`.`direct_name` AS `direct_name`,`t_mz_filmbasicinfo`.`actor_name` AS `actor_name`,`t_mz_filmbasicinfo`.`language` AS `language`,`t_mz_filmbasicinfo`.`release_date` AS `release_date`,`t_mz_filmbasicinfo`.`hImg` AS `hImg`,`t_mz_filmbasicinfo`.`vImg` AS `vImg`,`t_mz_filmbasicinfo`.`information` AS `information`,`t_mz_filmbasicinfo`.`play_counts` AS `play_counts`,`t_mz_filmbasicinfo`.`poster_path` AS `poster_path`,`t_mz_filmbasicinfo`.`status` AS `status`,`t_mz_filmbasicinfo`.`license` AS `license`,`t_mz_filmbasicinfo`.`tag` AS `tag`,`rec_group_category_hot`.`group_id` AS `groupId`,`rec_group_category_hot`.`rating` AS `groupRating`,`rec_group_category_hot`.`license` AS `groupLicense` from (`rec_group_category_hot` left join `t_mz_filmbasicinfo` on((`rec_group_category_hot`.`video_id` = `t_mz_filmbasicinfo`.`video_id`))) where (`t_mz_filmbasicinfo`.`video_id` is not null) ;

-- ----------------------------
-- View structure for v_wholeNetSearch
-- ----------------------------
DROP VIEW IF EXISTS `v_wholeNetSearch`;
CREATE ALGORITHM=UNDEFINED DEFINER=`root`@`%` SQL SECURITY DEFINER VIEW `v_wholeNetSearch` AS select `t_mz_filmbasicinfo`.`id` AS `id`,`t_mz_filmbasicinfo`.`video_id` AS `video_id`,`t_mz_filmbasicinfo`.`video_name` AS `video_name`,`t_mz_filmbasicinfo`.`video_time_length` AS `video_time_length`,`t_mz_filmbasicinfo`.`video_type` AS `video_type`,`t_mz_filmbasicinfo`.`video_plot` AS `video_plot`,(case when (`t_mz_filmbasicinfo`.`video_region` like '%中国%') then '中国大陆' when (`t_mz_filmbasicinfo`.`video_region` like '%内地%') then '中国大陆' when (`t_mz_filmbasicinfo`.`video_region` like '%台湾%') then '中国台湾' when (`t_mz_filmbasicinfo`.`video_region` like '%日本%') then '日本' when (`t_mz_filmbasicinfo`.`video_region` like '%法国%') then '法国' when (`t_mz_filmbasicinfo`.`video_region` like '%德国%') then '德国' when (`t_mz_filmbasicinfo`.`video_region` like '%意大利%') then '意大利' when (`t_mz_filmbasicinfo`.`video_region` like '%印度%') then '印度' when (`t_mz_filmbasicinfo`.`video_region` like '%泰国%') then '泰国' else '其他' end) AS `video_region`,`t_mz_filmbasicinfo`.`direct_name` AS `direct_name`,`t_mz_filmbasicinfo`.`actor_name` AS `actor_name`,`t_mz_filmbasicinfo`.`language` AS `language`,(case when (`t_mz_filmbasicinfo`.`release_date` like '%200%') then '00年代' when (`t_mz_filmbasicinfo`.`release_date` like '%199%') then '90年代' when (`t_mz_filmbasicinfo`.`release_date` like '%198%') then '其他' when (`t_mz_filmbasicinfo`.`release_date` like '%197%') then '其他' when (`t_mz_filmbasicinfo`.`release_date` like '%196%') then '其他' when (`t_mz_filmbasicinfo`.`release_date` like '%195%') then '其他' else `t_mz_filmbasicinfo`.`release_date` end) AS `release_date`,`t_mz_filmbasicinfo`.`hImg` AS `hImg`,`t_mz_filmbasicinfo`.`vImg` AS `vImg`,`t_mz_filmbasicinfo`.`information` AS `information`,`t_mz_filmbasicinfo`.`play_counts` AS `play_counts`,`t_mz_filmbasicinfo`.`poster_path` AS `poster_path`,`t_mz_filmbasicinfo`.`status` AS `status`,`t_mz_filmbasicinfo`.`license` AS `license`,`t_mz_filmbasicinfo`.`tag` AS `tag` from `t_mz_filmbasicinfo` order by `t_mz_filmbasicinfo`.`play_counts` desc ;
