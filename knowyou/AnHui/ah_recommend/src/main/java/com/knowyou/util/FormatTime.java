package com.knowyou.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class FormatTime {
    private final static SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
    private final static SimpleDateFormat sdfymdhm = new SimpleDateFormat("yyyyMMddHHmmss");
    private final static SimpleDateFormat sdfymdhms =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    /**
     *
     * @Title: getCurrentDay
     * @Description: TODO 获取当天时间(20161109)
     * @return
     */
    public static String getCurrentDay(){
        return sdf.format(new Date());
    }
    /**
     *
     * @Title: fTime2
     * @Description: TODO 获取time这个日期以前dayAgo天的日期
     * @return
     */
    public static String fTime(String time,int dayAgo){
        Date date = null;
        try {
            date = sdf.parse(time);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        if(dayAgo>0){
            calendar.add(Calendar.DAY_OF_MONTH, -dayAgo);//前15天数据
            date = calendar.getTime();
            calendar.setTime(date);
        }
        int year=calendar.get(Calendar.YEAR);
        int month=calendar.get(Calendar.MONTH) + 1;
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        String mon="";
        String d="";
        if(month<10){
            mon="0"+month;
        }else{
            mon=month+"";
        }
        if(day<10){
            d="0"+day;
        }else{
            d=""+day;
        }
        String ret=year+""+mon+""+d;
        return ret;
    }
    /**
     *
     * @Title: fTime2
     * @Description: TODO 获取time这个日期以后dayAfter天的日期
     * @return
     */
    public static String fTime2(String time,int dayAfter){
        Date date = null;
        try {
            date = sdf.parse(time);
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        if(dayAfter>0){
            calendar.add(Calendar.DAY_OF_MONTH, +dayAfter);//后15天数据
            date = calendar.getTime();
            calendar.setTime(date);
        }
        int year=calendar.get(Calendar.YEAR);
        int month=calendar.get(Calendar.MONTH) + 1;
        int day = calendar.get(Calendar.DAY_OF_MONTH);
        String mon="";
        String d="";
        if(month<10){
            mon="0"+month;
        }else{
            mon=month+"";
        }
        if(day<10){
            d="0"+day;
        }else{
            d=""+day;
        }
        String ret=year+""+mon+""+d;
        return ret;
    }
    /**
     *
     * @Title: getDefaultTime
     * @Description: TODO 获取昨天的日期
     * @return
     */
    public static String getDefaultTime(){
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_MONTH, -1);//前1天
        Date date = calendar.getTime();
        String time=sdf.format(date);
        return time;
    }
    /**
     *
     * @Title: getSunday
     * @Description: TODO 获取最近一个星期天
     * @return
     */
    public static String getSunday(){
        SimpleDateFormat f = new SimpleDateFormat("yyyyMMdd");
        Calendar c = Calendar.getInstance();
        c.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
        return f.format(c.getTime());
    }
    /**
     *
     * @Title: getMonthFirstDay
     * @Description: TODO 获取本月第一天
     * @return
     */
    public static String getCurrentMonthFirstDay(){
        Calendar  cal_1=Calendar.getInstance();//获取当前日期
        cal_1.add(Calendar.MONTH, 0);
        cal_1.set(Calendar.DAY_OF_MONTH,1);//设置为1号,当前日期既为本月第一天
        String firstDay = sdf.format(cal_1.getTime());
        return firstDay;
    }
    /**
     *
     * @Title: getMonthFirstDay
     * @Description: TODO 获取上月第一天
     * @return
     */
    public static String getPreviousMonthFirstDay(){
        //获取当前月第一天：
        Calendar c = Calendar.getInstance();
        c.add(Calendar.MONTH, -1);
        c.set(Calendar.DAY_OF_MONTH,1);//设置为1号,当前日期既为本月第一天
        String first = sdf.format(c.getTime());
        return first;
    }
    /**
     *
     * @Title: getMonthFirstDay
     * @Description: TODO 获取上月最后一天
     * @return
     */
    public static String getPreviousMonthLastDay(){
        //获取当前月最后一天
        Calendar ca = Calendar.getInstance();
        ca.set(Calendar.DAY_OF_MONTH,0);//
        String lastDay = sdf.format(ca.getTime());
        return lastDay;
    }
    /**
     *
     * @Title: getCurrentMonthLastDay
     * @Description: TODO 获取指定时间最后一天
     * @return
     */
    public static String getCurrentMonthLastDay(String time){
        Date date =null;
        try {
            date= sdf.parse(time);
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        //获取当前月最后一天
        Calendar ca = Calendar.getInstance();
        ca.setTime(date);
        ca.set(Calendar.DAY_OF_MONTH,
                ca.getActualMaximum(Calendar.DAY_OF_MONTH)); //
        String lastDay = sdf.format(ca.getTime());
        return lastDay;
    }
    /***
     *
     * @Title: getCurrentWeekDay
     * @Description: TODO 获取本周周一
     */
    public static String getCurrentMonday(){
        Calendar cal = Calendar.getInstance();
        cal.setFirstDayOfWeek(Calendar.MONDAY);//将每周第一天设为星期一，默认是星期天
        cal.add(Calendar.DATE, 0);
        cal.set(Calendar.DAY_OF_WEEK,Calendar.MONDAY);
        String monday = sdf.format(cal.getTime());
        return monday;
    }
    /***
     *
     * @Title: getPreviousSunday
     * @Description: TODO 获取上周周日
     */
    public static String getPreviousSunday(){
        Calendar cal = Calendar.getInstance();
        cal.setFirstDayOfWeek(Calendar.MONDAY);//将每周第一天设为星期一，默认是星期天
        cal.add(Calendar.DATE, -1*7);
        cal.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
        String sunday =sdf.format(cal.getTime());
        return sunday;
    }
    /**
     *
     * @Title: getMiniSencond
     * @Description: TODO 将日期转换为毫秒数
     * @param str
     * @return
     */
    public static String getMiniSencond(String str){
        long millionSeconds=0;
        try {
            millionSeconds = sdfymdhm.parse(str).getTime();//毫秒
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return millionSeconds+"";
    }
    /**
     *
     * @Title: getDateSencond
     * @Description: TODO 将日期转换为毫秒数
     * @param str
     * @return
     */
    public static long getDateSencond(String str){
        long millionSeconds=0;
        try {
            millionSeconds = sdfymdhms.parse(str).getTime();//毫秒
        } catch (ParseException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return millionSeconds;
    }
    /**
     * 计算日期相差天数
     * @param str1
     * @param str2
     * @return
     */
    public static int getDistanceOfTwoDate(String str1,String str2){
        int result=0;
        try{
            Date date1 = sdf.parse(str1);
            Date date2 =sdf.parse(str2);
            Calendar aCalendar = Calendar.getInstance();
            aCalendar.setTime(date1);
            int day1 = aCalendar.get(Calendar.DAY_OF_YEAR);
            aCalendar.setTime(date2);
            int day2 = aCalendar.get(Calendar.DAY_OF_YEAR);
            result = day1-day2;
        }catch(Exception e){
            e.printStackTrace();
        }
        return result;
    }
    /**
     *
     * @Title: long2Date
     * @Description: TODO long 转日期(年-月-日 时-分-秒)
     * @param timestamp
     * @return
     */
    public static String longToDate(Long msecond){
        Date date = new Date(msecond);
        return sdfymdhms.format(date);
    }
}
