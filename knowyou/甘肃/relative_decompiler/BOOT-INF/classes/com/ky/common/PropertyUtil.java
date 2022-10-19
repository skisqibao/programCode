package com.ky.common;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyUtil extends Properties {
   private static final long serialVersionUID = 50440463580273222L;
   private static PropertyUtil instance = null;

   public static synchronized PropertyUtil getInstance() {
      if (instance == null) {
         instance = new PropertyUtil();
      }

      return instance;
   }

   public String getProperty(String key, String defaultValue) {
      String val = this.getProperty(key);
      return val != null && !val.isEmpty() ? val : defaultValue;
   }

   public String getString(String name, String defaultValue) {
      return this.getProperty(name, defaultValue);
   }

   public int getInt(String name, int defaultValue) {
      String val = this.getProperty(name);
      return val != null && !val.isEmpty() ? Integer.parseInt(val) : defaultValue;
   }

   public long getLong(String name, long defaultValue) {
      String val = this.getProperty(name);
      return val != null && !val.isEmpty() ? Long.valueOf(val) : defaultValue;
   }

   public float getFloat(String name, float defaultValue) {
      String val = this.getProperty(name);
      return val != null && !val.isEmpty() ? Float.parseFloat(val) : defaultValue;
   }

   public double getDouble(String name, double defaultValue) {
      String val = this.getProperty(name);
      return val != null && !val.isEmpty() ? Double.parseDouble(val) : defaultValue;
   }

   public byte getByte(String name, byte defaultValue) {
      String val = this.getProperty(name);
      return val != null && !val.isEmpty() ? Byte.parseByte(val) : defaultValue;
   }

   public PropertyUtil() {
      try {
         InputStream in = this.getClass().getClassLoader().getResourceAsStream("application.properties");
         this.load(in);
         if (in != null) {
            in.close();
         }
      } catch (IOException var3) {
         var3.printStackTrace();
      }

   }
}
