# hive -f export_sql -hivevar C_DAY=20221211

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_serv_basic'
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY '#'
SELECT  
    deviceid,
    is_boot,
    is_play,
    play_duration,
    play_num,
    is_live,
    live_play_num,
    live_play_duration,
    is_demand,
    demand_play_num,
    demand_play_duration,
    is_replay,
    replay_play_num,
    replay_play_duration,
    date_time
FROM knowyou_ott_dmt.htv_serv_basic WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_serv_basic_month'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,is_boot
    ,is_play
    ,play_duration
    ,play_num
    ,is_live
    ,live_play_num
    ,live_play_duration
    ,is_demand
    ,demand_play_num
    ,demand_play_duration
    ,is_replay
    ,replay_play_num
    ,replay_play_duration
    ,date_time
FROM knowyou_ott_dmt.htv_serv_basic_month WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_live_prefer'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,live_prefer_lv1
    ,live_prefer_lv2
    ,live_prefer_lv3
    ,live_prefer_lv4
    ,live_prefer_lv5
    ,live_prefer_lv6
    ,live_prefer_lv7
    ,date_time
FROM knowyou_ott_dmt.htv_live_prefer WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_live_prefer_month'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,live_prefer_lv1
    ,live_prefer_lv2
    ,live_prefer_lv3
    ,live_prefer_lv4
    ,live_prefer_lv5
    ,live_prefer_lv6
    ,live_prefer_lv7
    ,date_time
FROM knowyou_ott_dmt.htv_live_prefer_month WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_demand_prefer'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,demand_prefer_lv1
    ,demand_prefer_lv2
    ,demand_prefer_lv3
    ,demand_prefer_lv4
    ,demand_prefer_lv5
    ,demand_prefer_lv6
    ,demand_prefer_lv7
    ,date_time
FROM knowyou_ott_dmt.htv_demand_prefer WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_demand_prefer_month'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,demand_prefer_lv1
    ,demand_prefer_lv2
    ,demand_prefer_lv3
    ,demand_prefer_lv4
    ,demand_prefer_lv5
    ,demand_prefer_lv6
    ,demand_prefer_lv7
    ,date_time
FROM knowyou_ott_dmt.htv_demand_prefer_month WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_replay_prefer'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,replay_prefer_lv1
    ,replay_prefer_lv2
    ,replay_prefer_lv3
    ,replay_prefer_lv4
    ,replay_prefer_lv5
    ,replay_prefer_lv6
    ,replay_prefer_lv7
    ,date_time
FROM knowyou_ott_dmt.htv_replay_prefer WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_replay_prefer_month'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,replay_prefer_lv1
    ,replay_prefer_lv2
    ,replay_prefer_lv3
    ,replay_prefer_lv4
    ,replay_prefer_lv5
    ,replay_prefer_lv6
    ,replay_prefer_lv7
    ,date_time
FROM knowyou_ott_dmt.htv_replay_prefer_month WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_channel_prefer_cctv_dm'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,channel_prefer_cctv1
    ,channel_low_prefer_cctv1
    ,channel_middle_prefer_cctv1
    ,channel_high_prefer_cctv1
    ,channel_prefer_cctv2
    ,channel_low_prefer_cctv2
    ,channel_middle_prefer_cctv2
    ,channel_high_prefer_cctv2
    ,channel_prefer_cctv3
    ,channel_low_prefer_cctv3
    ,channel_middle_prefer_cctv3
    ,channel_high_prefer_cctv3
    ,channel_prefer_cctv4
    ,channel_low_prefer_cctv4
    ,channel_middle_prefer_cctv4
    ,channel_high_prefer_cctv4
    ,channel_prefer_cctv5
    ,channel_low_prefer_cctv5
    ,channel_middle_prefer_cctv5
    ,channel_high_prefer_cctv5
    ,channel_prefer_cctv6
    ,channel_low_prefer_cctv6
    ,channel_middle_prefer_cctv6
    ,channel_high_prefer_cctv6
    ,channel_prefer_cctv7
    ,channel_low_prefer_cctv7
    ,channel_middle_prefer_cctv7
    ,channel_high_prefer_cctv7
    ,channel_prefer_cctv8
    ,channel_low_prefer_cctv8
    ,channel_middle_prefer_cctv8
    ,channel_high_prefer_cctv8
    ,channel_prefer_cctv9
    ,channel_low_prefer_cctv9
    ,channel_middle_prefer_cctv9
    ,channel_high_prefer_cctv9
    ,channel_prefer_cctv10
    ,channel_low_prefer_cctv10
    ,channel_middle_prefer_cctv10
    ,channel_high_prefer_cctv10
    ,channel_prefer_cctv11
    ,channel_low_prefer_cctv11
    ,channel_middle_prefer_cctv11
    ,channel_high_prefer_cctv11
    ,channel_prefer_cctv12
    ,channel_low_prefer_cctv12
    ,channel_middle_prefer_cctv12
    ,channel_high_prefer_cctv12
    ,channel_prefer_cctv13
    ,channel_low_prefer_cctv13
    ,channel_middle_prefer_cctv13
    ,channel_high_prefer_cctv13
    ,channel_prefer_cctv14
    ,channel_low_prefer_cctv14
    ,channel_middle_prefer_cctv14
    ,channel_high_prefer_cctv14
    ,channel_prefer_cctv15
    ,channel_low_prefer_cctv15
    ,channel_middle_prefer_cctv15
    ,channel_high_prefer_cctv15
    ,channel_prefer_cctv16
    ,channel_low_prefer_cctv16
    ,channel_middle_prefer_cctv16
    ,channel_high_prefer_cctv16
    ,channel_prefer_cctv17
    ,channel_low_prefer_cctv17
    ,channel_middle_prefer_cctv17
    ,channel_high_prefer_cctv17
    ,date_time
FROM knowyou_ott_dmt.htv_channel_prefer_cctv_dm WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_channel_prefer_dm'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,channel_prefer_lv1
    ,channel_prefer_lv2
    ,channel_prefer_lv3
    ,channel_prefer_lv4
    ,channel_prefer_lv5
    ,channel_prefer_lv6
    ,channel_prefer_lv7
    ,channel_prefer_lv8
    ,channel_prefer_lv9
    ,channel_prefer_lv10
    ,channel_prefer_lv11
    ,channel_prefer_lv12
    ,channel_prefer_lv13
    ,channel_prefer_lv14
    ,channel_prefer_lv15
    ,channel_prefer_lv16
    ,channel_prefer_lv17
    ,channel_prefer_lv18
    ,channel_prefer_lv19
    ,channel_prefer_lv20
    ,channel_prefer_lv21
    ,channel_prefer_lv22
    ,channel_prefer_lv23
    ,channel_prefer_lv24
    ,date_time
FROM knowyou_ott_dmt.htv_channel_prefer_dm WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_demand_type_prefer_dm'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,demand_type_prefer_lv1
    ,demand_type_prefer_lv2
    ,demand_type_prefer_lv3
    ,demand_type_prefer_lv4
    ,demand_type_prefer_lv5
    ,demand_type_prefer_lv6
    ,demand_type_prefer_lv7
    ,demand_type_prefer_lv8
    ,demand_type_prefer_lv9
    ,demand_type_prefer_lv10
    ,demand_type_prefer_lv11
    ,demand_type_prefer_lv12
    ,demand_type_prefer_lv13
    ,demand_type_prefer_lv14
    ,demand_type_prefer_lv15
    ,demand_type_prefer_lv16
    ,demand_type_prefer_lv17
    ,date_time
FROM knowyou_ott_dmt.htv_demand_type_prefer_dm WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_demand_column_prefer_dm'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,demand_type_prefer_lv1
    ,demand_type_prefer_lv2
    ,demand_type_prefer_lv3
    ,demand_type_prefer_lv4
    ,demand_type_prefer_lv5
    ,demand_type_prefer_lv6
    ,demand_type_prefer_lv7
    ,demand_type_prefer_lv8
    ,demand_type_prefer_lv9
    ,demand_type_prefer_lv10
    ,demand_type_prefer_lv11
    ,demand_type_prefer_other
    ,date_time
FROM knowyou_ott_dmt.htv_demand_column_prefer_dm WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_demand_column_prefer_dm_month'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,demand_type_prefer_lv1
    ,demand_type_prefer_lv2
    ,demand_type_prefer_lv3
    ,demand_type_prefer_lv4
    ,demand_type_prefer_lv5
    ,demand_type_prefer_lv6
    ,demand_type_prefer_lv7
    ,demand_type_prefer_lv8
    ,demand_type_prefer_lv9
    ,demand_type_prefer_lv10
    ,demand_type_prefer_lv11
    ,demand_type_prefer_other
    ,date_time
FROM knowyou_ott_dmt.htv_demand_column_prefer_dm_month WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_adduserretentionlabe_dm'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,morrow_adduser
    ,seven_adduser
    ,fourteen_adduser
    ,thirty_adduser
    ,date_time
FROM knowyou_ott_dmt.htv_adduserretentionlabe_dm WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_activeretentionlabe_dm'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,morrow_activer
    ,seven_activer
    ,fourteen_activer
    ,thirty_activer
    ,date_time
FROM knowyou_ott_dmt.htv_activeretentionlabe_dm WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_activeadd_retention_mm'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deal_time
    ,deviceid
    ,month_activer
    ,month_adduer
    ,date_time
FROM knowyou_ott_dmt.htv_activeadd_retention_mm WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_back_flow_lv7'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,date_time
FROM knowyou_ott_dmt.htv_back_flow_lv7 WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_back_flow_lv14'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,date_time
FROM knowyou_ott_dmt.htv_back_flow_lv14 WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_back_flow_lv30'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,date_time
FROM knowyou_ott_dmt.htv_back_flow_lv30 WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_slience_dm'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,pre7_slience_user
    ,pre14_slience_user
    ,pre30_slience_user
    ,pre_month_slience_user
    ,date_time
FROM knowyou_ott_dmt.htv_slience_dm WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_live_content_light'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,videoname
    ,date_time
FROM knowyou_ott_dmt.htv_live_content_light WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_live_content_middle'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,videoname
    ,date_time
FROM knowyou_ott_dmt.htv_live_content_middle WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_live_content_high'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,videoname
    ,date_time
FROM knowyou_ott_dmt.htv_live_content_high WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_demand_content_light'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,videoname
    ,date_time
FROM knowyou_ott_dmt.htv_demand_content_light WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_demand_content_middle'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,videoname
    ,date_time
FROM knowyou_ott_dmt.htv_demand_content_middle WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_demand_content_high'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,videoname
    ,date_time
FROM knowyou_ott_dmt.htv_demand_content_high WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_searchcollectlabe_dm'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    tag
    ,videoname
    ,playnums
    ,date_time
FROM knowyou_ott_dmt.htv_searchcollectlabe_dm WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_plotlabel_dm'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,v_b1
    ,v_b2
    ,v_b3
    ,v_b4
    ,v_b5
    ,v_b6
    ,v_b7
    ,v_b8
    ,v_b9
    ,v_b10
    ,v_b11
    ,v_b12
    ,v_b13
    ,v_b14
    ,v_b15
    ,v_b16
    ,v_b17
    ,v_b18
    ,v_b19
    ,v_b20
    ,v_b21
    ,v_b22
    ,v_b23
    ,v_b24
    ,v_b25
    ,v_b26
    ,v_b27
    ,v_b28
    ,v_b29
    ,v_b30
    ,v_b31
    ,v_b32
    ,v_b33
    ,v_b34
    ,date_time
FROM knowyou_ott_dmt.htv_plotlabel_dm WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_regionlabel_dm'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,v_b1
    ,v_b2
    ,v_b3
    ,v_b4
    ,v_b5
    ,v_b6
    ,v_b7
    ,v_b8
    ,v_b9
    ,v_b10
    ,v_b11
    ,v_b12
    ,v_b13
    ,v_b14
    ,date_time
FROM knowyou_ott_dmt.htv_regionlabel_dm WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_yearslabel_dm'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,v_b1
    ,v_b2
    ,v_b3
    ,v_b4
    ,v_b5
    ,v_b6
    ,v_b7
    ,v_b8
    ,v_b9
    ,v_b10
    ,v_b11
    ,date_time
FROM knowyou_ott_dmt.htv_yearslabel_dm WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_languagelabel_dm'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,v_b1
    ,v_b2
    ,v_b3
    ,v_b4
    ,v_b5
    ,v_b6
    ,v_b7
    ,v_b8
    ,date_time
FROM knowyou_ott_dmt.htv_languagelabel_dm WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_actorlabel_dm'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,actorname
    ,date_time
FROM knowyou_ott_dmt.htv_actorlabel_dm WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_directorlabel_dm'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,directorname
    ,date_time
FROM knowyou_ott_dmt.htv_directorlabel_dm WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_demand_order_dm'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,film_not_order
    ,tv_not_order
    ,child_not_order
    ,date_time
FROM knowyou_ott_dmt.htv_demand_order_dm WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_film_order_dm'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,film_light_not_order
    ,film_middle_not_order
    ,film_high_not_order
    ,date_time
FROM knowyou_ott_dmt.htv_film_order_dm WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_tv_order_dm'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,tv_light_not_order
    ,tv_middle_not_order
    ,tv_high_not_order
    ,date_time
FROM knowyou_ott_dmt.htv_tv_order_dm WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_child_order_dm'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,child_light_not_order
    ,child_middle_not_order
    ,child_high_not_order
    ,date_time
FROM knowyou_ott_dmt.htv_child_order_dm WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_secondlevel_increase'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    deviceid
    ,film
    ,child
    ,comic
    ,esport
    ,game
    ,edu
    ,health
    ,life
    ,sport
    ,platform
    ,music
    ,other
    ,date_time
FROM knowyou_ott_dmt.htv_secondlevel_increase WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/htv_arpu_dm'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    arpu_five
    ,arpu_ten
    ,arpu_twenty
    ,arpu_thirty
    ,date_time
FROM knowyou_ott_dmt.htv_arpu_dm WHERE date_time=${hivevar:C_DAY} ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/hx_activetime_rank'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    device_id
    ,activetime0
    ,activetime1
    ,activetime10
    ,activetime11
    ,activetime12
    ,activetime13
    ,activetime14
    ,activetime15
    ,activetime16
    ,activetime17
    ,activetime18
    ,activetime19
    ,activetime2
    ,activetime20
    ,activetime21
    ,activetime22
    ,activetime23
    ,activetime3
    ,activetime4
    ,activetime5
    ,activetime6
    ,activetime7
    ,activetime8
    ,activetime9
FROM hive_hbase_table_mapping.hx_activetime_rank ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/hx_content_analysis'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    device_id
    ,dianbo
    ,huikan
    ,live
FROM hive_hbase_table_mapping.hx_content_analysis ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/hx_content_label'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    device_id
    ,video_top1
    ,video_top10
    ,video_top2
    ,video_top3
    ,video_top4
    ,video_top5
    ,video_top6
    ,video_top7
    ,video_top8
    ,video_top9
FROM hive_hbase_table_mapping.hx_content_label ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/hx_dianbo_rank'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    device_id
    ,children
    ,edu
    ,entertainment
    ,film
    ,life
    ,record
    ,sports
    ,tv
    ,variety
FROM hive_hbase_table_mapping.hx_dianbo_rank ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/hx_live_rank'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    device_id
    ,live_playnum1
    ,live_playnum10
    ,live_playnum2
    ,live_playnum3
    ,live_playnum4
    ,live_playnum5
    ,live_playnum6
    ,live_playnum7
    ,live_playnum8
    ,live_playnum9
    ,live_top1
    ,live_top10
    ,live_top2
    ,live_top3
    ,live_top4
    ,live_top5
    ,live_top6
    ,live_top7
    ,live_top8
    ,live_top9
FROM hive_hbase_table_mapping.hx_live_rank ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/hx_portraittag'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    device_id
    ,family_label
    ,tj_active_level
    ,tj_switch_frequency
    ,tj_watch_frequency
FROM hive_hbase_table_mapping.hx_portraittag ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/rec_guess_3'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    device_id
    ,rec_list
FROM hive_hbase_table_mapping.rec_guess_3 ;

INSERT OVERWRITE local DIRECTORY '/tmp/label/rec_guess_32'
ROW FORMAT DELIMITED FIELDS TERMINATED BY '#'
SELECT  
    device_id
    ,rec_list
FROM hive_hbase_table_mapping.rec_guess_32 ;