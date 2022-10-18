package com.knowyou.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Properties;

/**
 * Created by wengxw on 2019/11/25
 * 配置文件工具类
 */
public class Property {

    private final static Logger log = LoggerFactory.getLogger(Property.class);

    private final static String CONF_NAME = "config.properties";

    private static Properties contextProperties;

    static {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream(CONF_NAME);
        contextProperties = new Properties();
        try {
            InputStreamReader inputStreamReader = new InputStreamReader(in, "UTF-8");
            contextProperties.load(inputStreamReader);
        } catch (IOException e) {
            log.error(">>>flink-Recommand<<<资源文件加载失败：" + e.getMessage());
            e.printStackTrace();
        }
        log.info(">>>flink-Recommand<<<资源加载成功 ");
    }

    public static String getStrValue(String key) {
        return contextProperties.getProperty(key);
    }

    public static int getIntValue(String key) {
        String strValue = getStrValue(key);
        //此处不做校验，暂且认为不会出错
        return Integer.parseInt(strValue);
    }

    public static Properties getKafkaProperties(String groupId) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", getStrValue("kafka.bootstrap.servers"));
        properties.setProperty("zookeeper.connect", getStrValue("kafka.zookeeper.connect"));
        properties.setProperty("auto.offset.reset", getStrValue("auto.offset.reset"));
        properties.setProperty("group.id", groupId);
        return properties;
    }

}
