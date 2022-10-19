package com.knowyou.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import org.apache.hadoop.hbase.security.token.TokenUtil;

public class DateUtil {
    /**
     * 获取昨天日期
     * @return
     */
    public static String getYesterday(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE,-2);
        String yesterday = sdf.format(calendar.getTime());
        return yesterday;
    }
    public static String getOrderYesterday(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMM");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE,-2);
        String yesterday = sdf.format(calendar.getTime());
        return yesterday;
    }


    public static void main(String[] args) {
        String yesterday = getYesterday();
        System.out.println(yesterday);

    }

    public static String getTwoweeks(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE,-15);
        String twoweeks = sdf.format(calendar.getTime());
        return twoweeks;
    }
}
