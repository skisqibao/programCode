package com.ky.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @Author: xwj
 * @Date: 2019/2/19 0019 14:42
 * @Version 1.0
 */
public class PropertyUtil extends Properties {

    private static final long serialVersionUID = 50440463580273222L;

    private static PropertyUtil instance = null;

    public static synchronized PropertyUtil getInstance() {
        if (instance == null) {
            instance = new PropertyUtil();
        }
        return instance;
    }

    @Override
    public String getProperty(String key, String defaultValue) {
        String val = getProperty(key);
        return (val == null || val.isEmpty()) ? defaultValue : val;
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
        return (val == null || val.isEmpty()) ? defaultValue : Long.valueOf(val);
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

    public PropertyUtil() {
        InputStream in;
        try {
            in = this.getClass().getClassLoader().getResourceAsStream("application.properties");
            this.load(in);
            if (in != null) {
                in.close();
            }
        } catch (IOException e1) {
            // TODO Auto-generated catch block
            e1.printStackTrace();
        }

    }

}
