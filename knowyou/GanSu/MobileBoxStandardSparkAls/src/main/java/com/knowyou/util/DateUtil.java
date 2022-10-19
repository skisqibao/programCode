package com.knowyou.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class DateUtil {
    /**
     * 获取昨天日期
     * @return
     */
    public static String getYesterday(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE,-1);
        String yesterday = sdf.format(calendar.getTime());
        return yesterday;
    }

    public static String getTheDayBeforeYesterday(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE,-2);
        String yesterday = sdf.format(calendar.getTime());
        return yesterday;
    }

    public static String getTwoweeks(){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DATE,-15);
        String twoweeks = sdf.format(calendar.getTime());
        return twoweeks;
    }

    public static void main(String[] args) {
        String yesterday = getTheDayBeforeYesterday();
        System.out.println(yesterday);

        String t_week = getTwoweeks();
        System.out.println(t_week);
    }

}
