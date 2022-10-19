package com.knowyou

import java.util.Properties

import org.apache.spark.sql.{DataFrame, SparkSession}

object Meizi {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder()
      .appName("Meizi")
      //      .master("local[*]")
      .enableHiveSupport()
      .getOrCreate()
    val props = new Properties()
    props.load(Thread.currentThread().getContextClassLoader.getResourceAsStream("config.properties"))
    //    val meiziSql = "select j.seriesheadcode as rowkey,j.updatetype,j.cpid,j.seriesname,j.contentcode,j.contentname,j.seriesnum,j.videotype,j.elapsedtime,j.resolution,j.genre,j.createtime,j.description,j.directors,j.actors,j.orgairdate,j.country,j.language,j.score,j.catalog,j.seriesheadcode,j.pictureurl as picurl,j.apkurl,j.cls,j.pkg,j.cornerurl,j.cornerposition,j.cpid as productcpid from \n(select g.*,h.apkurl,h.cls,h.pkg,h.cornerurl,h.cornerposition,h.cpid as productcpid,row_number() over(partition by g.seriesheadcode  order by g.orgairdate) as rank from\n(select e.*,f.productname from\n(select c.*,d.productcode from \n(select a.*,b.pictureurl from knowyou_ott_edw.edw_mz_relative_content_dm a \nleft join \n(select seriesheadcode,pictureurl from knowyou_ott_ods.ods_mz_seriespic_dm )b \non a.seriesheadcode=b.seriesheadcode where dt=20210812)c\nleft join \n(select productcode,contentcode from knowyou_ott_ods.ods_mz_productcontent_dm )d \non c.contentcode=d.contentcode)e \nleft join \n(select productcode,productname from knowyou_ott_ods.ods_mz_product_dm)f \non e.productcode=f.productcode)g \nleft join \nknowyou_ott_ods.ods_product_map h\non g.cpid=substr(h.cmsid,5,8) and g.productcode=h.product_code)j where j.rank=1 "
    //    val meiziSql = "select j.seriesheadcode as rowkey,j.updatetype,j.cpid,j.seriesname,j.contentcode,j.contentname,j.seriesnum,j.videotype,j.elapsedtime,j.resolution,j.genre,j.createtime,j.description,j.directors,j.actors,j.orgairdate,j.country,j.language,j.score,j.catalog,j.seriesheadcode,j.pictureurl as picurl,j.apkurl,j.cls,j.pkg,j.cornerurl,j.cornerposition,j.cpid as productcpid from \n(select g.*,h.apkurl,h.cls,h.pkg,h.cornerurl,h.cornerposition,h.cpid as productcpid,row_number() over(partition by g.seriesheadcode  order by g.orgairdate) as rank from\n(select e.*,f.productname from\n(select c.*,d.productcode from \n(select a.*,b.pictureurl from knowyou_ott_edw.edw_mz_relative_content_dm a \nleft join \n(select seriesheadcode,pictureurl from knowyou_ott_ods.ods_mz_seriespic_dm )b \non a.seriesheadcode=b.seriesheadcode where dt=20210812)c\nleft join \n(select productcode,contentcode from knowyou_ott_ods.ods_mz_productcontent_dm )d \non c.contentcode=d.contentcode)e \nleft join \n(select productcode,productname from knowyou_ott_ods.ods_mz_product_dm)f \non e.productcode=f.productcode)g \ninner join \nknowyou_ott_ods.ods_product_map h\non g.cpid=substr(h.cmsid,5,8) and g.productcode=h.product_code)j where j.rank=1"
    //    val meiziSql = "select aa.seriesheadcode as rowkey,aa.updatetype,aa.cpid,aa.seriesname,aa.contentcode,aa.contentname,aa.seriesnum,aa.videotype,aa.elapsedtime,aa.resolution,aa.genre,aa.createtime,aa.description,aa.directors,aa.actors,aa.orgairdate,aa.country,aa.language,aa.score,aa.catalog,aa.seriesheadcode,aa.pictureurl as picurl,h.apkurl,h.cls,h.pkg,h.cornerurl,h.cornerposition,h.cpid as productcpid from ( \nselect c.*,\nCOALESCE (d.productcode,k.productcode,\"\") as productcode\n from \n(select a.*,b.pictureurl from knowyou_ott_edw.edw_mz_relative_content_dm a \nleft join \n(select seriesheadcode,max(pictureurl) as pictureurl from knowyou_ott_ods.ods_mz_seriespic_dm group by seriesheadcode)b \non a.seriesheadcode=b.seriesheadcode where a.dt in (select max(dt) from knowyou_ott_edw.edw_mz_relative_content_dm ))c\nleft join \n(select distinct contentcode as contentcode, productcode from knowyou_ott_ods.ods_mz_productcontent_dm )d \non c.contentcode=d.contentcode \nleft join \n(select distinct contentcode as contentcode, productcode from knowyou_ott_ods.ods_mz_productcontent_dm )k \non  c.seriesheadcode=k.contentcode )aa \nleft join \nknowyou_ott_ods.ods_product_map h \non aa.cpid=h.cmscode and aa.productcode=h.product_code "

    // 中兴数据更换为讯飞
    val meiziSql = "SELECT NVL(a.mediaid, 0) AS rowkey, 0 AS updatetype, NVL(a.cmsid, 0) AS cpid, NVL(a.name, 0) AS seriesname, 0 AS contentcode, NVL(a.name, 0) AS contentname, 0 AS seriesnum, 0 AS videotype, 0 AS elapsedtime, 0 AS resolution, NVL(a.type, 0) AS genre, NVL(a.updatetime, 0) AS createtime, NVL(a.introduction, 0) AS description, NVL(a.director, 0) AS directors, NVL(a.actor, 0) AS actors, 0 AS orgairdate, NVL(a.region, 0) AS country, NVL(a.language, 0) AS language, 0 AS score, NVL(a.category, 0) AS catalog, NVL(a.mediaid, 0) AS seriesheadcode, NVL(a.thumbnails, 0) AS picurl, NVL(b.apkurl, 0) AS apkurl, NVL(a.packagename, 0) AS cls, NVL(a.classname, 0) AS pkg, NVL(b.cornerurl, 0) AS cornerurl, NVL(b.cornerposition, 0) AS cornerposition, NVL(a.productid, 0) AS productcpid  FROM knowyou_ott_edw.edw_xunfei_media_all a  LEFT JOIN knowyou_ott_ods.ods_product_map b  ON a.cmsid = b.cmsid  AND a.productid = b.product_code GROUP BY a.mediaid, a.cmsid, a.name, a.name, a.type, a.updatetime, a.introduction, a.director, a.actor, a.region, a.language, a.category, a.mediaid, a.thumbnails, b.apkurl, a.packagename, a.classname, b.cornerurl, b.cornerposition, a.productid"

    val frame = sparkSession.sql(String.format(meiziSql)).cache()
    frame.show(50)

    val tableName = "ahMeizi"
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "updatetype")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "cpid")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "seriesname")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "contentcode")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "contentname")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "seriesnum")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "videotype")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "elapsedtime")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "resolution")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "genre")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "createtime")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "description")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "directors")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "actors")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "orgairdate")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "country")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "language")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "score")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "catalog")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "seriesheadcode")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "picurl")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "apkurl")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "cls")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "pkg")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "cornerurl")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "cornerposition")
    SaveDataToHbaseScala.saveAsNewAPIHadoopDataSet(sparkSession, frame, tableName, "productcpid")
    frame.unpersist()
    sparkSession.close()
  }
}
