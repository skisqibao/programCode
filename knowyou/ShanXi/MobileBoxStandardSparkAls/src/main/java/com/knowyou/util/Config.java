package com.knowyou.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @Author: Xnone
 * @Date: 2020/2/20 0020 11:32
 */
public class Config extends Properties {

    private static final long serialVersionUID = 7879897914378883356L;
    private static Config instance = null;

    public static synchronized Config getInstance() {
        if (instance == null) {
            instance = new Config();
        }
        return instance;
    }

    @Override
    public String getProperty(String key, String defaultValue) {
        String value = getProperty(key);
        return (value == null || value.isEmpty()) ? defaultValue : value;
    }

    public String getString(String name, String defaultValue) {
        return this.getProperty(name, defaultValue);
    }

    public int getInt(String name, int defaultValue) {
        String val = this.getProperty(name);
        return (val == null || val.isEmpty()) ? defaultValue : Integer.parseInt(val);
    }

    public long getLong(String name, long defaultValue) {
        String val = this.getProperty(name);
        return (val == null || val.isEmpty()) ? defaultValue : Integer.parseInt(val);
    }

    public float getFloat(String name, float defaultValue) {
        String val = this.getProperty(name);
        return (val == null || val.isEmpty()) ? defaultValue : Float.parseFloat(val);
    }

    public double getDouble(String name, double defaultValue) {
        String val = this.getProperty(name);
        return (val == null || val.isEmpty()) ? defaultValue : Double.parseDouble(val);
    }

    public byte getByte(String name, byte defaultValue) {
        String val = this.getProperty(name);
        return (val == null || val.isEmpty()) ? defaultValue : Byte.parseByte(val);
    }

    public  Boolean getBoolean(String name,Boolean defaultValue){
        String val = this.getProperty(name);
        return (val == null || val.isEmpty())?defaultValue:Boolean.parseBoolean(val);
    }

    public Config() {
        InputStream in;
        try {
            in = this.getClass().getClassLoader().getResourceAsStream(UtilConstants.Public.CONFIG_FILE_NAME);
            this.load(in);
            in.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


    public static void main(String[] args) {
        Config instance = Config.getInstance();
        String pwd = instance.getString("IS_RMSE", "");
        System.out.println(pwd);

    }
}
