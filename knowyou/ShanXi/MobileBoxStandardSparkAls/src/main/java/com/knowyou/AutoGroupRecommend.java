package com.knowyou;

import com.knowyou.model.RecGroupUserListModel;
import com.knowyou.util.Config;
import com.knowyou.util.DateUtil;
import com.knowyou.util.MysqlHelper;
import com.knowyou.util.UtilConstants;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;

public class AutoGroupRecommend {

    private static final Logger log = LoggerFactory.getLogger(AutoGroupRecommend.class);
    //Kmeans 模型HDFS保存路径
    private static String KMEANS_MODEL_HDFS_PATH;
    //所有牌照方
    private static String ALS_LICENSE;
    //ALS模型HDFS保存路径
    private static String ALS_MODEL_HDFS_PATH;
    //Kmeans聚类分组数
    private static int KMEANS_NUM_CLUSTERS;
    //Kmeans聚类迭代次数
    private static int KMEANS_NUM_ITERATIONS;
    //用户与自增id关系rdd存储目录
    private static String USER_TRANSLATE_HDFS_PATH;
    //原始数据日分区时间
    private static String ALS_DATA_TIME;
    //输入数据数据库
    private static String ALS_DATABASE;
    //决定是否手动选择日期
    private static Boolean ALS_DATA_AUTO;

    public static void main(String[] args) {
        Config config = Config.getInstance();

        ALS_LICENSE = config.getProperty(UtilConstants.AlsRec.ALS_LICENSE, "3");
        ALS_MODEL_HDFS_PATH = config.getProperty(UtilConstants.AlsRec.ALS_MODEL_HDFS_PATH, "0");
        KMEANS_MODEL_HDFS_PATH = config.getProperty(UtilConstants.KmeansRec.KMEANS_MODEL_HDFS_PATH, "0");
        KMEANS_NUM_CLUSTERS = config.getInt(UtilConstants.KmeansRec.KMEANS_NUM_CLUSTERS, 5);
        KMEANS_NUM_ITERATIONS = config.getInt(UtilConstants.KmeansRec.KMEANS_NUM_ITERATIONS, 20);
        USER_TRANSLATE_HDFS_PATH = config.getProperty(UtilConstants.AlsRec.USER_TRANSLATE_HDFS_PATH, "0");
        ALS_DATA_AUTO = config.getBoolean(UtilConstants.AlsRec.ALS_DATA_AUTO,false);
        if(ALS_DATA_AUTO){
            ALS_DATA_TIME = DateUtil.getYesterday();
        } else {
            ALS_DATA_TIME = config.getProperty(UtilConstants.AlsRec.ALS_DATA_TIME, "20200921");
        }
        ALS_DATABASE = config.getProperty(UtilConstants.AlsRec.ALS_DATABASE, "default");
        String yesterday = DateUtil.getYesterday();

        SparkSession ss = SparkSession.builder()
                .appName("AutoGroupRecommend")
                .enableHiveSupport()
                .getOrCreate();
        JavaSparkContext jsc = JavaSparkContext.fromSparkContext(ss.sparkContext());


        //切分牌照方，遍历牌照方根据不同数据源进行算法模型计算
        String[] licenseSplit = ALS_LICENSE.split(",");
        int licenseNum = licenseSplit.length;
        for (int n = 0; n < licenseNum; n++) {
            String licenseName = licenseSplit[n];

            //获得用户自增id,用户id的关系
            JavaPairRDD<String, String> userTranslateRDD = jsc.textFile(String.format(USER_TRANSLATE_HDFS_PATH, licenseName))
                    .mapToPair(new PairFunction<String, String, String>() {
                        @Override
                        public Tuple2<String, String> call(String s) throws Exception {
                            String[] splits = s.split(",");
                            //设备id
                            String deviceId = splits[0];
                            //自增设备id
                            String deviceIncreaseId = splits[1];
                            return new Tuple2<>(deviceIncreaseId, deviceId);
                        }
                    });

            MatrixFactorizationModel model = MatrixFactorizationModel.load(jsc.sc(), String.format(ALS_MODEL_HDFS_PATH, licenseName));
            JavaRDD<Tuple2<Object, double[]>> userFeaturesRDD = model.userFeatures().toJavaRDD();
            JavaRDD<Vector> parsedData = userFeaturesRDD.map(new Function<Tuple2<Object, double[]>, Vector>() {
                @Override
                public Vector call(Tuple2<Object, double[]> v1) throws Exception {
                    String user = String.valueOf(v1._1);
                    double[] values = v1._2;

                    return Vectors.dense(values);
                }
            });

            parsedData.cache();

            KMeansModel clusters = KMeans.train(parsedData.rdd(), KMEANS_NUM_CLUSTERS, KMEANS_NUM_ITERATIONS);
            // 模型保存
            clusters.save(jsc.sc(), String.format(KMEANS_MODEL_HDFS_PATH, licenseName));
            //自增id分组PairRDD
            JavaPairRDD<String, Integer> increaseDeviceIdGroupRddPairRDD = userFeaturesRDD
                    .mapToPair(new PairFunction<Tuple2<Object, double[]>, String, Integer>() {
                        @Override
                        public Tuple2<String, Integer> call(Tuple2<Object, double[]> v1) throws Exception {
                            String increaseDeviceId = String.valueOf(v1._1);
                            int predictGroupNum = clusters.predict(Vectors.dense(v1._2));
                            return new Tuple2<>(increaseDeviceId, predictGroupNum);
                        }
                    });

            //得到用户id对应分组序号 写入Mysql表
            JavaPairRDD<String, Integer> deviceIdGroupPairRDD = increaseDeviceIdGroupRddPairRDD
                    .leftOuterJoin(userTranslateRDD)
                    .mapToPair(new PairFunction<Tuple2<String, Tuple2<Integer, Optional<String>>>, String, Integer>() {
                        @Override
                        public Tuple2<String, Integer> call(Tuple2<String, Tuple2<Integer, Optional<String>>> v1) throws Exception {
                            //原始设备id
                            String deviceId = v1._2._2.orElse("unknown");
                            //分组序号
                            Integer groupNum = v1._2._1;
                            return new Tuple2<>(deviceId, groupNum);
                        }
                    });
            //分组用户数<组id，用户数>
            JavaPairRDD<Integer, Integer> groupCountRDD = deviceIdGroupPairRDD.mapToPair(x -> new Tuple2<>(x._2, 1)).reduceByKey(new Function2<Integer, Integer, Integer>() {
                @Override
                public Integer call(Integer v1, Integer v2) throws Exception {
                    return v1 + v2;
                }
            });
            JavaRDD<Row> groupCountRow = groupCountRDD.map(x -> RowFactory.create(x._1, x._2));
            ArrayList<StructField> groupCountStructFields = new ArrayList<>();
            groupCountStructFields.add(DataTypes.createStructField("group_id", DataTypes.IntegerType, true));
            groupCountStructFields.add(DataTypes.createStructField("user_num", DataTypes.IntegerType, true));
            StructType groupCountStructType = DataTypes.createStructType(groupCountStructFields);
            //创建分组用户数临时表
            Dataset<Row> groupCountDF = ss.createDataFrame(groupCountRow, groupCountStructType);
            groupCountDF.createOrReplaceTempView("groupCount");
            String groupCountDF_sql = String.format("insert into rec");
            MysqlHelper.dfInsert(groupCountDF, "Append", "groupcount");

            JavaRDD<Row> recGroupUserListRow = deviceIdGroupPairRDD.map(new Function<Tuple2<String, Integer>, Row>() {
                @Override
                public Row call(Tuple2<String, Integer> v1) throws Exception {
                    return RowFactory.create(v1._2, v1._1, licenseName, yesterday);
                }
            });
            ArrayList<StructField> recGroupUserStructFields = new ArrayList<>();
            recGroupUserStructFields.add(DataTypes.createStructField("group_id", DataTypes.IntegerType, true));
            recGroupUserStructFields.add(DataTypes.createStructField("device_id", DataTypes.StringType, true));
            recGroupUserStructFields.add(DataTypes.createStructField("license", DataTypes.StringType, true));
            recGroupUserStructFields.add(DataTypes.createStructField("date_time", DataTypes.StringType, true));
            StructType recGroupUserStructType = DataTypes.createStructType(recGroupUserStructFields);
            Dataset<Row> recGroupUserListDF = ss.createDataFrame(recGroupUserListRow, recGroupUserStructType);
            recGroupUserListDF.createOrReplaceTempView("rec_group_user_list_tmp");
            //山西写入hive
            ss.sql("insert into table knowyou_ott.rec_group_user_list select * from rec_group_user_list_tmp");
//            MysqlHelper.dfInsert(recGroupUserListDF, "Append", "rec_group_user_list");

            recGroupUserListDF.createOrReplaceTempView("recGroupUserList");
//            System.out.println("=======================recGroupUserList==================================");
//            recGroupUserListDF.show(10);
            //计算rec_group_category_hot组内热门并写入mysql
            String recGroupCategoryHotDataSql = String
                    .format(" select m.group_id,m.video_id,m.category,m.rating,m.license,m.date_time " +
                                    "from( select *,row_number() over(partition by tt.group_id,tt.category order by tt.rating desc)as rank " +
                                    "from( select t.group_id,t.video_id,t.content_type as category, sum(t.watch_count) as rating ,t.license,t.date_time " +
                                    "from( select t0.video_id,t0.watch_count,t0.content_type,t1.group_id,t0.license,t0.dt as date_time " +
                                    "from( select device_id ,video_id ,watch_count ,content_type,license,dt from %s.ads_rec_userdetail_wtr " +
                                    "where license ='%s' and dt='%s' )t0 left join( select group_id,device_id ,license,date_time  " +
                                    "from recGroupUserList  " +
                                    ")t1 on t0.device_id=t1.device_id )t group by t.group_id , t.video_id , t.content_type " +
                                    ", t.license , t.date_time )tt )m where m.rank<=50 "
                            , ALS_DATABASE ,licenseName, ALS_DATA_TIME);
            Dataset<Row> recGroupCategoryHotDataDF = ss.sql(recGroupCategoryHotDataSql);
            recGroupCategoryHotDataDF.createOrReplaceTempView("rec_group_category_hot_tmp");
            //山西写入hive
            ss.sql("insert into table knowyou_ott.rec_group_category_hot select * from rec_group_category_hot_tmp");
//            MysqlHelper.dfInsert(recGroupCategoryHotDataDF, "Append", "rec_group_category_hot");

            //计算组内热门
            String recGroupHotDataSql = String.format("SELECT m.group_id, m.video_id,  m.rating, m.license, m.date_time " +
                            "FROM ( SELECT *, row_number() over ( PARTITION BY tt.group_id ORDER BY tt.rating DESC ) AS rank " +
                            "FROM ( SELECT t.group_id, t.video_id, sum( t.watch_count ) AS rating, t.license, t.date_time " +
                            "FROM ( SELECT t0.video_id, t0.watch_count, t1.group_id, t0.license, t0.dt AS date_time " +
                            "FROM ( SELECT device_id, video_id, watch_count, license, dt FROM %s.ads_rec_userdetail_wtr " +
                            "WHERE license = '%s' AND dt = '%s' ) t0 LEFT JOIN ( SELECT group_id, device_id FROM recGroupUserList " +
                            ") t1 ON t0.device_id = t1.device_id ) t GROUP BY t.group_id, t.video_id, t.license, t.date_time ) tt ) m " +
                            "WHERE m.rank <= 40 "
                    , ALS_DATABASE, licenseName, ALS_DATA_TIME);
            Dataset<Row> recGroupHotDataDF = ss.sql(recGroupHotDataSql);
            recGroupHotDataDF.createOrReplaceTempView("rec_group_hot");

            //计算rec_group_guess组内猜你喜欢并写入mysql
            Dataset<Row> groupVideoRatingDF = ss.sql("select t0.group_id,t0.video_id,(cast(t0.rating as double)/cast(t1.user_num as double)) as fix_rating from rec_group_hot t0 left join ( select group_id,user_num from groupCount )t1 on t0.group_id=t1.group_id");
            groupVideoRatingDF.createOrReplaceTempView("groupVideoRating");
            groupVideoRatingDF.show(200);
//            Dataset<Row> videoAllRatingDF = ss.sql("select a.group_id,a.video_id,(a.fix_rating/b.all_rating+a.fix_rating/10000) as rating from groupVideoRating a left join ( select video_id,sum(fix_rating) as all_rating from groupVideoRating group by video_id )b on a.video_id=b.video_id");
            Dataset<Row> videoAllRatingDF = ss.sql("select a.group_id,a.video_id,( cast(a.fix_rating as double)/ cast(b.all_rating as double) +cast(a.fix_rating as double)/10000 ) as rating from groupVideoRating a left join ( select video_id,sum(fix_rating) as all_rating from groupVideoRating group by video_id )b on a.video_id=b.video_id");
            videoAllRatingDF.createOrReplaceTempView("videoAllRating");
            Dataset<Row> recGroupGuess = ss.sql(String.format("select t.group_id,t.video_id,rating,'%s' as license, '%s' as date_time " +
                            "from( select *,row_number() over(partition by group_id order by rating desc)as rank from  videoAllRating)t where t.rank<=24"
                    , licenseName, ALS_DATA_TIME));
            recGroupGuess.createOrReplaceTempView("rec_group_guess_tmp");
            //山西写入hive
            ss.sql("insert into table knowyou_ott.rec_group_guess select * from rec_group_guess_tmp");
//            MysqlHelper.dfInsert(recGroupGuess, "Append", "rec_group_guess");

            //计算rec_group_category_order组内类别顺序表并写入mysql
            String recGroupCategoryOrderDataSql = String
                    .format("select t.group_id,t.content_type as category, sum(t.watch_count) as watchcount ,t.license,t.date_time " +
                                    "from( select t0.video_id,t0.watch_count,t0.content_type,t1.group_id,t0.license,t0.date_time " +
                                    "from( select device_id ,video_id ,watch_count ,content_type,license,dt as date_time from %s.ads_rec_userdetail_wtr " +
                                    "where license ='%s' and dt='%s' )t0 left join( select group_id,device_id ,license,date_time  " +
                                    "from recGroupUserList )t1 on t0.device_id=t1.device_id )t " +
                                    "group by t.group_id ,t.content_type,t.license , t.date_time"
                            , ALS_DATABASE,licenseName, ALS_DATA_TIME );

            Dataset<Row> recGroupCategoryOrderDataDF = ss.sql(recGroupCategoryOrderDataSql);
            recGroupCategoryOrderDataDF.createOrReplaceTempView("rec_group_category_order_tmp");
            //山西写入hive
            ss.sql("insert into table knowyou_ott.rec_group_category_order select * from rec_group_category_order_tmp");
//            MysqlHelper.dfInsert(recGroupCategoryOrderDataDF, "Append", "rec_group_category_order");

            //计算rec_group_label组内标签结果表并写入mysql
            String recGroupLabelDataSql = String
                    .format("select t.group_id, round((sum(t.watch_count)/count(distinct t.device_id)),4)  as avg_watchcount," +
                                    " round((sum(t.watch_duration)/count(distinct t.device_id)/3600),4) as avg_watchtime,t.license,t.date_time " +
                                    "from( select t0.device_id,t0.watch_count,t0.watch_duration,t1.group_id,t0.license,t0.dt as date_time " +
                                    "from( select device_id ,watch_count ,watch_duration ,license,dt " +
                                    "from %s.ads_rec_userdetail_wtr where license ='%s' and dt='%s' )t0 " +
                                    "left join( select group_id,device_id ,license,date_time  " +
                                    "from recGroupUserList  " +
                                    ")t1 on t0.device_id=t1.device_id " +
                                    ")t group by t.group_id ,t.license , t.date_time"
                            , ALS_DATABASE, licenseName, ALS_DATA_TIME );

            Dataset<Row> recGroupLabelDataDF = ss.sql(recGroupLabelDataSql);
            recGroupLabelDataDF.createOrReplaceTempView("rec_group_label_tmp");
            //山西写入hive
            ss.sql("insert into table knowyou_ott.rec_group_label select * from rec_group_label_tmp");
//            MysqlHelper.dfInsert(recGroupLabelDataDF, "Append", "rec_group_label");


        }
        jsc.close();
        ss.close();
    }
}
