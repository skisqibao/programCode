CREATE EXTERNAL TABLE `knowyou_ott_ods.ods_series_info_map`(
  `series` string COMMENT '', 
  `series_code` string COMMENT '', 
  `series_action` string COMMENT '', 
  `series_name` string COMMENT '', 
  `series_ordernumber` string COMMENT '', 
  `series_originalname` string COMMENT '', 
  `series_sortname` string COMMENT '', 
  `series_searchname` string COMMENT '', 
  `series_orgairdate` string COMMENT '', 
  `series_licensingwindowstart` string COMMENT '', 
  `series_licensingwindowend` string COMMENT '', 
  `series_displayasnew` string COMMENT '', 
  `series_displayaslastchance` string COMMENT '', 
  `series_macrovision` string COMMENT '', 
  `series_price` string COMMENT '', 
  `series_volumncount` string COMMENT '', 
  `series_status` string COMMENT '', 
  `series_description` string COMMENT '', 
  `series_type` string COMMENT '', 
  `series_rmediacode` string COMMENT '', 
  `series_titlesc` string COMMENT '', 
  `series_score` string COMMENT '', 
  `series_imagesc` string COMMENT '', 
  `series_updatetimesc` string COMMENT '', 
  `series_cmsid` string COMMENT '', 
  `series_result` string COMMENT '', 
  `series_errordescription` string COMMENT '', 
  `series_isfree` string COMMENT '', 
  `series_director` string COMMENT '', 
  `series_actordisplay` string COMMENT '', 
  `series_originalcountry` string COMMENT '', 
  `series_language` string COMMENT '', 
  `series_keywords` string COMMENT '', 
  `package_series` string COMMENT '', 
  `createtime` string COMMENT '', 
  `updatetime` string COMMENT '', 
  `picture_series_1_1_fileurl` string COMMENT '', 
  `series_reserve1` string COMMENT '', 
  `series_reserve2` string COMMENT '', 
  `series_reserve3` string COMMENT '', 
  `series_reserve4` string COMMENT '', 
  `series_reserve5` string COMMENT '', 
  `picture_series_1_1_height` string COMMENT '', 
  `picture_series_1_1_rate` string COMMENT '', 
  `picture_series_1_1_width` string COMMENT '',

  `movie_duration` string COMMENT 'New Fields from here to end.2022.07.25',
  `movie_resolution` string COMMENT '',
  `movie_type` string COMMENT '',
  `package_series_action` string COMMENT '',
  `package_series_elementcode` string COMMENT '',
  `package_series_id` string COMMENT '',
  `package_series_parentcode` string COMMENT '',
  `program_actordisplay` string COMMENT '',
  `program_bcharging` string COMMENT '',
  `program_cmsid` string COMMENT '',
  `program_code` string COMMENT '',
  `program_description` string COMMENT '',
  `program_director` string COMMENT '',
  `program_genre` string COMMENT '',
  `program_language` string COMMENT '',
  `program_movie` string COMMENT '',
  `program_movie_action` string COMMENT '',
  `program_movie_elementcode` string COMMENT '',
  `program_movie_id` string COMMENT '',
  `program_movie_parentcode` string COMMENT '',
  `program_movie_sequence` string COMMENT '',
  `program_movie_type` string COMMENT '',
  `program_name` string COMMENT '',
  `program_originalcountry` string COMMENT '',
  `program_periods` string COMMENT '',
  `program_releaseyear` string COMMENT '',
  `program_seriesflag` string COMMENT '',
  `program_status` string COMMENT '',
  `program_type` string COMMENT '',
  `program_writerdisplay` string COMMENT '',
  `series_genre` string COMMENT '',
  `series_program` string COMMENT '',
  `series_program_action` string COMMENT '',
  `series_program_elementcode` string COMMENT '',
  `series_program_id` string COMMENT '',
  `series_program_parentcode` string COMMENT '',
  `series_program_sequence` string COMMENT '',
  `series_program_type` string COMMENT '',
  `series_rating` string COMMENT '',
  `series_scoresc` string COMMENT ''
  )
ROW FORMAT SERDE 'org.apache.hadoop.hive.hbase.HBaseSerDe' 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' 
WITH SERDEPROPERTIES ( 
  'hbase.columns.mapping'=':key, 
  info:Series_Code, 
  info:Series_Action, 
  info:Series_Name, 
  info:Series_OrderNumber, 
  info:Series_OriginalName, 
  info:Series_SortName, 
  info:Series_SearchName, 
  info:Series_OrgAirDate , 
  info:Series_LicensingWindowStart , 
  info:Series_LicensingWindowEnd   , 
  info:Series_DisplayAsNew   , 
  info:Series_DisplayAsLastChance  , 
  info:Series_Macrovision    , 
  info:Series_Price    , 
  info:Series_VolumnCount   , 
  info:Series_Status    , 
  info:Series_Description , 
  info:Series_Type    , 
  info:Series_RMediaCode, 
  info:Series_TitleSC  , 
  info:Series_Score   , 
  info:Series_ImageSC  , 
  info:Series_UpdateTimeSC , 
  info:Series_CMSID    , 
  info:Series_Result  , 
  info:Series_ErrorDescription   , 
  info:Series_IsFree   , 
  info:Series_Director  , 
  info:Series_ActorDisplay , 
  info:Series_OriginalCountry  , 
  info:Series_Language , 
  info:Series_Keywords  , 
  info:Package_Series   , 
  info:CreateTime    , 
  info:UpdateTime    , 
  info:Picture_Series_1_1_FileURL, 
  info:Series_Reserve1 , 
  info:Series_Reserve2 , 
  info:Series_Reserve3 , 
  info:Series_Reserve4 , 
  info:Series_ReleaseYear,
  info:Picture_Series_1_1_Height,
  info:Picture_Series_1_1_Rate,
  info:Picture_Series_1_1_Width,

  info:Movie_Duration ,
  info:Movie_Resolution , 
  info:Movie_Type , 
  info:Package_Series_Action , 
  info:Package_Series_ElementCode ,  
  info:Package_Series_ID , 
  info:Package_Series_ParentCode ,  
  info:Program_ActorDisplay , 
  info:Program_Bcharging , 
  info:Program_CMSID , 
  info:Program_Code , 
  info:Program_Description , 
  info:Program_Director ,  
  info:Program_Genre , 
  info:Program_Language , 
  info:Program_Movie , 
  info:Program_Movie_Action , 
  info:Program_Movie_ElementCode ,  
  info:Program_Movie_ID , 
  info:Program_Movie_ParentCode ,  
  info:Program_Movie_Sequence , 
  info:Program_Movie_Type , 
  info:Program_Name , 
  info:Program_OriginalCountry , 
  info:Program_Periods , 
  info:Program_ReleaseYear , 
  info:Program_SeriesFlag ,  
  info:Program_Status , 
  info:Program_Type , 
  info:Program_WriterDisplay , 
  info:Series_Genre , 
  info:Series_Program , 
  info:Series_Program_Action , 
  info:Series_Program_ElementCode ,  
  info:Series_Program_ID , 
  info:Series_Program_ParentCode ,  
  info:Series_Program_Sequence , 
  info:Series_Program_Type , 
  info:Series_Rating,
  info:Series_ScoreSC', 
  'serialization.format'='1')
TBLPROPERTIES ('hbase.table.name'='t_series_info');



ALTER TABLE knowyou_ott_ods.dim_pub_video_df ADD COLUMNS(
  `movie_duration` string COMMENT 'New Fields from here to end.2022.07.25',
  `movie_resolution` string COMMENT '',
  `movie_type` string COMMENT '',
  `package_series_action` string COMMENT '',
  `package_series_elementcode` string COMMENT '',
  `package_series_id` string COMMENT '',
  `package_series_parentcode` string COMMENT '',
  `program_actordisplay` string COMMENT '',
  `program_bcharging` string COMMENT '',
  `program_cmsid` string COMMENT '',
  `program_code` string COMMENT '',
  `program_description` string COMMENT '',
  `program_director` string COMMENT '',
  `program_genre` string COMMENT '',
  `program_language` string COMMENT '',
  `program_movie` string COMMENT '',
  `program_movie_action` string COMMENT '',
  `program_movie_elementcode` string COMMENT '',
  `program_movie_id` string COMMENT '',
  `program_movie_parentcode` string COMMENT '',
  `program_movie_sequence` string COMMENT '',
  `program_movie_type` string COMMENT '',
  `program_name` string COMMENT '',
  `program_originalcountry` string COMMENT '',
  `program_periods` string COMMENT '',
  `program_releaseyear` string COMMENT '',
  `program_seriesflag` string COMMENT '',
  `program_status` string COMMENT '',
  `program_type` string COMMENT '',
  `program_writerdisplay` string COMMENT '',
  `series_genre` string COMMENT '',
  `series_program` string COMMENT '',
  `series_program_action` string COMMENT '',
  `series_program_elementcode` string COMMENT '',
  `series_program_id` string COMMENT '',
  `series_program_parentcode` string COMMENT '',
  `series_program_sequence` string COMMENT '',
  `series_program_type` string COMMENT '',
  `series_rating` string COMMENT '',
  `series_scoresc` string COMMENT ''
);



CREATE TABLE IF NOT EXISTS knowyou_ott_ods.dim_pub_video_df(
  series                         string,
  series_action                  string,
  series_name                    string,
  series_ordernumber             string,
  series_originalname            string,
  series_sortname                string,
  series_searchname              string,
  series_orgairdate              string,
  series_licensingwindowstart    string,
  series_licensingwindowend      string,
  series_displayasnew            string, 
  series_displayaslastchance     string,
  series_macrovision             string,
  series_price                   string,
  series_volumncount             string,
  series_status                  string,
  series_description             string,
  series_type                    string,
  series_rmediacode              string,
  series_titlesc                 string,
  series_score                   string,
  series_imagesc                 string,
  series_updatetimesc            string,
  series_cmsid                   string,
  series_result                  string,
  series_errordescription        string,
  series_isfree                  string,
  series_director                string,
  series_actordisplay            string,
  series_originalcountry         string,
  series_language                string,
  series_keywords                string,
  package_series                 string,
  createtime                     string,
  updatetime                     string,
  picture_series_1_1_fileurl     string,
  series_reserve1                string,
  series_reserve2                string,
  series_reserve3                string,
  series_reserve4                string,
  series_reserve5                string,
  video_type                     string,
  picture_series_1_1_height      string,
  picture_series_1_1_rate        string,
  picture_series_1_1_width       string,

  `movie_duration`               string COMMENT 'New Fields from here to end.2022.07.25',
  `movie_resolution`             string COMMENT '',
  `movie_type`                   string COMMENT '',
  `package_series_action`        string COMMENT '',
  `package_series_elementcode `  string COMMENT '',
  `package_series_id`            string COMMENT '',
  `package_series_parentcode`    string COMMENT '',
  `program_actordisplay`         string COMMENT '',
  `program_bcharging`            string COMMENT '',
  `program_cmsid`                string COMMENT '',
  `program_code`                 string COMMENT '',
  `program_description`          string COMMENT '',
  `program_director`             string COMMENT '',
  `program_genre`                string COMMENT '',
  `program_language`             string COMMENT '',
  `program_movie`                string COMMENT '',
  `program_movie_action`         string COMMENT '',
  `program_movie_elementcode`    string COMMENT '',
  `program_movie_id`             string COMMENT '',
  `program_movie_parentcode`     string COMMENT '',
  `program_movie_sequence`       string COMMENT '',
  `program_movie_type`           string COMMENT '',
  `program_name`                 string COMMENT '',
  `program_originalcountry`      string COMMENT '',
  `program_periods`              string COMMENT '',
  `program_releaseyear`          string COMMENT '',
  `program_seriesflag`           string COMMENT '',
  `program_status`               string COMMENT '',
  `program_type`                 string COMMENT '',
  `program_writerdisplay`        string COMMENT '',
  `series_genre`                 string COMMENT '',
  `series_program`               string COMMENT '',
  `series_program_action`        string COMMENT '',
  `series_program_elementcode `  string COMMENT '',
  `series_program_id`            string COMMENT '',
  `series_program_parentcode`    string COMMENT '',
  `series_program_sequence`      string COMMENT '',
  `series_program_type`          string COMMENT '',
  `series_rating`                string COMMENT '',
  `series_scoresc`               string COMMENT ''
)
PARTITIONED BY (dt string)
STORED AS orc tblproperties("orc.compress"="SNAPPY");




,movie_duration
,movie_resolution
,movie_type
,package_series_action
,package_series_elementcode
,package_series_id
,package_series_parentcode
,program_actordisplay
,program_bcharging
,program_cmsid
,program_code
,program_description
,program_director
,program_genre
,program_language
,program_movie
,program_movie_action
,program_movie_elementcode
,program_movie_id
,program_movie_parentcode
,program_movie_sequence
,program_movie_type
,program_name
,program_originalcountry
,program_periods
,program_releaseyear
,program_seriesflag
,program_status
,program_type
,program_writerdisplay
,series_genre
,series_program
,series_program_action
,series_program_elementcode
,series_program_id
,series_program_parentcode
,series_program_sequence
,series_program_type
,series_rating
,series_scoresc


ALTER TABLE knowyou_ott_ods.dws_rec_video_playinfo_di ADD COLUMNS(
  series_duration      string  COMMENT 'New Fields from here to end.2022.07.27',
  series_resolution    string  COMMENT '',
  movie_type           string  COMMENT '',
  series_rating        string  COMMENT ''
)

INSERT overwrite TABLE knowyou_ott_ods.dws_rec_video_playinfo_di partition(dt='${C_DAY}')
select
  '探针' as data_source,
  a.deviceid,
  a.actualtime,
  a.provincecode,
  a.citycode,
  a.urlvideoid,
  a.jsonvideoid,
  a.actionpath,
  a.firstlevel,
  a.secondlevel,
  a.threelevel,
  a.entrytype,
  a.pannelpos,
  a.videotype,
  a.recommendedtype,
  a.programname,
  a.contentname,
  a.channelname,
  a.oldchannel,
  a.oldcontentname,
  a.programurl,
  a.playtime,
  a.contenttype,
  a.licensename,
  a.pkg,
  a.licenseversion,
  a.luaversion,
  b.series as seriesheadcode,
  coalesce(b.series_action, b.package_series_action) as series_action,
  coalesce(b.series_name, b.program_name) as series_name,
  b.series_searchname,
  coalesce(b.series_status, b.program_status) as series_status,
  coalesce(b.series_description, b.program_description) as series_description,
  b.video_type as series_type,
  coalesce(b.series_cmsid, b.program_cmsid),
  coalesce(b.series_isfree, b.program_bcharging),
  coalesce(b.series_director, b.program_director, b.program_writerdisplay),
  coalesce(b.series_actordisplay, b.program_actordisplay),
  coalesce(b.series_originalcountry, b.program_originalcountry),
  coalesce(b.series_language, b.program_language),
  coalesce(b.series_keywords, b.series_genre, b.program_genre),
  b.package_series,
  b.picture_series_1_1_fileurl,
  coalesce(b.series_reserve5, b.program_releaseyear) as series_reserve5,
  '' as program_merge_set,

  b.movie_duration as series_duration,
  b.movie_resolution as series_resolution,
  coalesce(b.movie_type, b.program_movie_type) as movie_type,
  coalesce(b.series_rating, b.series_scoresc) as series_rating
from 
(
  select * from knowyou_ott_ods.dwd_fct_log_play_jt_di
  where dt='${C_DAY}' and videotype='点播' and playstatus='退出'
)a
left join 
(
-- series_status =0 上线节目       series_status=1 下线节目
  select   
    *,
    (case when video_type='电视剧' and series_reserve5>=2000 then '1'
    when video_type='电影' and series_reserve5>=1993 then '1'
    when video_type='综艺' and series_reserve5>=2010 then '1'
    when video_type='少儿' and series_reserve5>=2008 then '1'
    when video_type='体育' and series_reserve5>=2016 then '1'
    when video_type='纪录片' and series_reserve5>=2011 then '1'
    when video_type='动漫' and series_reserve5>=2008 then '1'
    when video_type not in('电视剧','电影','综艺','少儿','体育','纪录片','动漫') and series_reserve5 >2004 then '1'
    else '0' end)as qc_type
  from knowyou_ott_ods.dim_pub_video_df 
  where series_status ='0' and picture_series_1_1_rate>=0.5 and picture_series_1_1_rate<=0.8
  and dt in (select max(dt) from knowyou_ott_ods.dim_pub_video_df) 
)b 
on a.seriesheadcode = b.series 
where b.series is not null and b.qc_type='1'

union all

select 
  (case when a.appname='' then 'APK' else a.appname end)  as data_source,
  a.deviceid,
  '' as actualtime,
  '' as provincecode,
  '' as citycode,
  '' as urlvideoid,
  '' as jsonvideoid,
  '' as actionpath,
  '' as firstlevel,
  '' as secondlevel,
  '' as threelevel,
  '' as entrytype,
  '' as pannelpos,
  '点播' as videotype,
  '' as recommendedtype,
  a.videoname as programname,
  '' as contentname,
  '' as channelname,
  '' as oldchannel,
  '' as oldcontentname,
  '' as programurl,
  a.videoplaytimecp as playtime,
  a.apptype as contenttype,
  '' as licensename,
  '' as pkg,
  '' as licenseversion,
  '' as luaversion,
  b.series as seriesheadcode,
  coalesce(b.series_action, b.package_series_action) as series_action,
  coalesce(b.series_name, b.program_name) as series_name,
  b.series_searchname,
  coalesce(b.series_status, b.program_status) as series_status,
  coalesce(b.series_description, b.program_description) as series_description,
  b.video_type as series_type,
  coalesce(b.series_cmsid, b.program_cmsid) as series_cmsid,
  coalesce(b.series_isfree, b.program_bcharging) as series_isfree,
  coalesce(b.series_director, b.program_director, b.program_writerdisplay) as series_director,
  coalesce(b.series_actordisplay, b.program_actordisplay) as series_actordisplay,
  coalesce(b.series_originalcountry, b.program_originalcountry) as series_originalcountry,
  coalesce(b.series_language, b.program_language) as series_language,
  coalesce(b.series_keywords, b.series_genre, b.program_genre) as series_keywords,
  b.package_series,
  b.picture_series_1_1_fileurl,
  coalesce(b.series_reserve5, b.program_releaseyear) as series_reserve5,
  '' as program_merge_set,

  b.movie_duration as series_duration,
  b.movie_resolution as series_resolution,
  coalesce(b.movie_type, b.program_movie_type) as movie_type,
  coalesce(b.series_rating, b.series_scoresc) as series_rating
from 
(
  select 
    deviceid,
    appname,
    apptype,
    videotype,
    videoname,
    videoplaytimecp,
    split(split(playurl,'productid=')[1],'&')[0] as playurl
  from knowyou_ott_ods.dwd_fct_log_play_cp_di
  where dt='${C_DAY}' and playstatus ='PLAY_END'
)a
left join 
(
  select 
    *,
    (case when video_type='电视剧' and series_reserve5>=2000 then '1'
    when video_type='电影' and series_reserve5>=1993 then '1'
    when video_type='综艺' and series_reserve5>=2010 then '1'
    when video_type='少儿' and series_reserve5>=2008 then '1'
    when video_type='体育' and series_reserve5>=2016 then '1'
    when video_type='纪录片' and series_reserve5>=2011 then '1'
    when video_type='动漫' and series_reserve5>=2008 then '1'
    when video_type not in('电视剧','电影','综艺','少儿','体育','纪录片','动漫') and series_reserve5 >2004 then '1'
    else '0' end)as qc_type
  from 
  (
    select 
      *, row_number() over(partition by series_name,package_series order by series desc)rank
    from knowyou_ott_ods.dim_pub_video_df 
    where series_status ='0' and picture_series_1_1_rate>=0.5 and picture_series_1_1_rate<=0.8
    and dt in(select max(dt)  from knowyou_ott_ods.dim_pub_video_df) 
  )bb 
  where bb.rank=1
)b
on a.videoname =b.series_name and a.playurl=b.package_series
where b.package_series is not null and b.qc_type='1'


ALTER TABLE knowyou_ott_ods.dws_all_video_playinfo_di ADD COLUMNS(
  series_duration      string  COMMENT 'New Fields from here to end.2022.07.27',
  series_resolution    string  COMMENT '',
  movie_type           string  COMMENT '',
  series_rating        string  COMMENT ''
)

INSERT overwrite TABLE knowyou_ott_ods.dws_all_video_playinfo_di partition(dt='${C_DAY}')
select
  '探针' as data_source,
  a.deviceid,
  a.actualtime,
  a.provincecode,
  a.citycode,
  a.urlvideoid,
  a.jsonvideoid,
  a.actionpath,
  a.firstlevel,
  a.secondlevel,
  a.threelevel,
  a.entrytype,
  a.pannelpos,
  a.videotype,
  a.recommendedtype,
  a.programname,
  a.contentname,
  a.channelname,
  a.oldchannel,
  a.oldcontentname,
  a.programurl,
  a.playtime,
  a.contenttype,
  a.licensename,
  a.pkg,
  a.licenseversion,
  a.luaversion,
  b.series as  seriesheadcode,
  coalesce(b.series_action, b.package_series_action) as series_action,
  coalesce(b.series_name, b.program_name) as series_name,
  b.series_searchname,
  coalesce(b.series_status, b.program_status) as series_status,
  coalesce(b.series_description, b.program_description) as series_description,
  b.video_type as series_type,
  coalesce(b.series_cmsid, b.program_cmsid) as series_cmsid,
  coalesce(b.series_isfree, b.program_bcharging) as series_isfree,
  coalesce(b.series_director, b.program_director, b.program_writerdisplay) as series_director,
  coalesce(b.series_actordisplay, b.program_actordisplay) as series_actordisplay,
  coalesce(b.series_originalcountry, b.program_originalcountry) as series_originalcountry,
  coalesce(b.series_language, b.program_language) as series_language,
  coalesce(b.series_keywords, b.series_genre, b.program_genre) as series_keywords,
  b.package_series,
  b.picture_series_1_1_fileurl,
  coalesce(b.series_reserve5, b.program_releaseyear) as series_reserve5,

  b.movie_duration as series_duration,
  b.movie_resolution as series_resolution,
  coalesce(b.movie_type, b.program_movie_type) as movie_type,
  coalesce(b.series_rating, b.series_scoresc) as series_rating
from 
(
  select * from knowyou_ott_ods.dwd_fct_log_play_jt_di
  where dt='${C_DAY}'  and playstatus='退出'
)a
left join 
(
  select * from knowyou_ott_ods.dim_pub_video_df 
  where dt in(select max(dt)  from knowyou_ott_ods.dim_pub_video_df) and series_status ='0' 
)b on a.seriesheadcode =b.series 
where b.series is not null 

union all

select 
  (case when a.appname='' then 'APK' else a.appname end)  as data_source,
  a.deviceid,
  '' as actualtime,
  '' as provincecode,
  '' as citycode,
  '' as urlvideoid,
  '' as jsonvideoid,
  '' as actionpath,
  '' as firstlevel,
  '' as secondlevel,
  '' as threelevel,
  '' as entrytype,
  '' as pannelpos,
  '点播' as videotype,
  '' as recommendedtype,
  a.videoname as programname,
  a.apptype as contentname,
  '' as channelname,
  '' as oldchannel,
  '' as oldcontentname,
  '' as programurl,
  a.videoplaytimecp as playtime,
  '' as contenttype,
  '' as licensename,
  '' as pkg,
  '' as licenseversion,
  '' as luaversion,
  b.series as seriesheadcode,
  coalesce(b.series_action, b.package_series_action) as series_action,
  coalesce(b.series_name, b.program_name) as series_name,
  b.series_searchname,
  coalesce(b.series_status, b.program_status) as series_status,
  coalesce(b.series_description, b.program_description) as series_description,
  b.video_type as series_type,
  coalesce(b.series_cmsid, b.program_cmsid) as series_cmsid,
  coalesce(b.series_isfree, b.program_bcharging) as series_isfree,
  coalesce(b.series_director, b.program_director, b.program_writerdisplay) as series_director,
  coalesce(b.series_actordisplay, b.program_actordisplay) as series_actordisplay,
  coalesce(b.series_originalcountry, b.program_originalcountry) as series_originalcountry,
  coalesce(b.series_language, b.program_language) as series_language,
  coalesce(b.series_keywords, b.series_genre, b.program_genre) as series_keywords,
  b.package_series,
  b.picture_series_1_1_fileurl,
  coalesce(b.series_reserve5, b.program_releaseyear) as series_reserve5,

  b.movie_duration as series_duration,
  b.movie_resolution as series_resolution,
  coalesce(b.movie_type, b.program_movie_type) as movie_type,
  coalesce(b.series_rating, b.series_scoresc) as series_rating
from 
(
  select 
    deviceid,
    appname,
    apptype,
    videotype,
    videoname,
    videoplaytimecp,
    split(split(playurl,'productid=')[1],'&')[0] as playurl
  from knowyou_ott_ods.dwd_fct_log_play_cp_di
  where dt='${C_DAY}' and playstatus ='PLAY_END'
)a
left join 
(
  select * from 
  (
    select 
      *,
      row_number() over(partition by series_name,package_series order by series desc) rank
    from knowyou_ott_ods.dim_pub_video_df 
    where dt in(select max(dt) from knowyou_ott_ods.dim_pub_video_df) and series_status ='0'  
  )bb 
  where bb.rank=1
)b
on a.videoname =b.series_name and a.playurl=b.package_series
where b.package_series is not null