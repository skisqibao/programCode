package com.knowyou.util;

/**
 * 常量接口
 */
public interface UtilConstants {
    /**
     * 公共常量
     */
    class Public {
        public static final String CONFIG_FILE_NAME = "config.properties";

        public static final String HBASE_ZOOKEEPER_QUORUM = "hbase.zookeeper.quorum";

        public static final String HBASE_ZOOKEEPER_CLIENTPORT = "hbase.zookeeper.property.clientport";

        public static final String HBASE_ZNODE_PARENT = "zookeeper.znode.parent";

        public static final String MYSQL_URL = "mysql.url";

        public static final String MYSQL_USER = "user";

        public static final String MYSQL_PASSWORD = "password";

        public static final String MYSQL_DRIVER = "driver";

    }

    /**
     * ALS相关常量
     */
    class AlsRec {
        public static final String ALS_DATABASE = "als.database";

        public static final String ALS_RANK = "als.rank";

        public static final String ALS_NUM_ITERATIONS = "als.numIterations";

        public static final String ALS_LAMBDA = "als.lambda";

        public static final String ALS_BATCH_SIZE = "als.batchSize";

        public static final String ALS_RELETIVE_RANK = "als.reletiveRank";

        public static final String ALS_REC_NUM = "als.recnum";

        public static final String ALS_LICENSE = "als.license";

        public static final String ALS_DATA_TIME = "als.dataTime";

        public static final String ALS_DATA_AUTO = "als.dataTimeAuto";

        public static final String ALS_ALPHA = "als.alpha";

        public static final String ALS_MODEL_HDFS_PATH = "als.modelHdfsPath";

        public static final String USER_TRANSLATE_HDFS_PATH = "als.userTranslateHdfsPath";

    }

    /**
     * Kmeans相关常量
     */
    class KmeansRec{
        public static final String KMEANS_MODEL_HDFS_PATH = "kmeans.modelHdfsPath";

        public static final String KMEANS_NUM_CLUSTERS = "kmeans.numClusters";

        public static final String KMEANS_NUM_ITERATIONS = "kmeans.numIterations";
    }

}
