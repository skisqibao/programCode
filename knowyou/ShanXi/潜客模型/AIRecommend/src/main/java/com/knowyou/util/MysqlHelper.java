package com.knowyou.util;

import com.knowyou.model.ImplicitALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;

public class MysqlHelper {
    private final static Logger log = LoggerFactory.getLogger(MysqlHelper.class);
    private static String MYSQL_URL;
    private static String MYSQL_USER;
    private static String MYSQL_PASSWORD;
    private static String MYSQL_DRIVER;

    static {
        try {
            final Config config = Config.getInstance();
            MYSQL_URL = config.getProperty(UtilConstants.Public.MYSQL_URL);
            MYSQL_USER = config.getProperty(UtilConstants.Public.MYSQL_USER);
            MYSQL_PASSWORD = config.getProperty(UtilConstants.Public.MYSQL_PASSWORD);
            MYSQL_DRIVER = config.getProperty(UtilConstants.Public.MYSQL_DRIVER);
        } catch (Exception e) {
            log.error("mysql Config Initialization failure,配置文件信息获取异常 ");
        }
    }

    /**
     * @param dataFrame
     * @param saveMode  {Append, Overwrite, ErrorIfExists, Ignore;}
     * @return
     */
    public static boolean dfInsert(Dataset<Row> dataFrame, String saveMode, String mysqlTable) {
        Properties properties = new Properties();
        properties.setProperty(UtilConstants.Public.MYSQL_USER, MYSQL_USER);
        properties.setProperty(UtilConstants.Public.MYSQL_PASSWORD, MYSQL_PASSWORD);
        properties.setProperty(UtilConstants.Public.MYSQL_DRIVER, MYSQL_DRIVER);
        dataFrame.write().mode(SaveMode.valueOf(saveMode)).jdbc(MYSQL_URL, mysqlTable, properties);
        return true;
    }

//    /**
//     * 通过二进制流的方式保存模型
//     *
//     * @param model     插入的模型对象，可能是不同算法的model
//     * @param modelType 插入的模型类型，作为数据库id
//     */
//    public static void saveModel(Object model, String modelType) {
//        try {
//            //定义一个字节数组输出流
//            ByteArrayOutputStream os = new ByteArrayOutputStream();
//            //定义一个对象输出流
//            ObjectOutputStream out = new ObjectOutputStream(os);
//            out.writeObject(model);
//            //byte[]
//            byte[] modeByte = os.toByteArray();
//            ImplicitALSModel implicitALSModel = new ImplicitALSModel();
//            implicitALSModel.setId(modelType);
//            implicitALSModel.setModel(modeByte);
//
//            SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
//            implicitALSModel.setCreateTime(sdf.format(new Date()));
//
//            os.close();
//            out.close();
//
//        } catch (IOException e) {
//            log.error(modelType + " saveModel failture ! ");
//            e.printStackTrace();
//        }
//    }
//
//    /**
//     *
//     * @param model
//     * @return
//     */
//    public static Object loadModel(ImplicitALSModel model){
//
//    }


}
