package com.ky.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.util.Arrays;
import java.util.List;

/**
 * HBase 各个组件管理调用类
 * 可以根据配置文件来选择  HBase 官方 API 还是第三方API
 */
public class HBase {

    private HBase() {
    }

    private static HBaseService hBaseService;

    static {
      
        hBaseService = new HBaseServiceImpl();
    }

    /**
     * 创建hbase表
     *
     * @param tableName      表名称
     * @param columnFamilies 列族
     * @param preBuildRegion 是否预分区
     */
    public static void createTable(String tableName, String[] columnFamilies, boolean preBuildRegion) {
        try {
            hBaseService.createTable(tableName, columnFamilies, preBuildRegion);
        } catch (Exception e) {
            System.out.println("create hbase table fail, the root cause is " + e.getMessage());
        }
    }

    /**
     * 写入单条数据
     *
     * @param tableName 表名称
     * @param put       列值
     * @param waiting   是否等待线程执行完成  true 可以及时看到结果, false 让线程继续执行，并跳出此方法返回调用方主程序
     * @return
     */
    public static void put(String tableName, Put put, boolean waiting) {
        hBaseService.batchPut(tableName, Arrays.asList(put), waiting);
    }

//    /**
//     * 多线程同步提交
//     *
//     * @param tableName 表名称
//     * @param puts      待提交参数
//     * @param waiting   是否等待线程执行完成  true 可以及时看到结果, false 让线程继续执行，并跳出此方法返回调用方主程序
//     */
//    public static void put(String tableName, List<Put> puts, boolean waiting) {
//        hBaseService.batchPut(tableName, puts, waiting);
//    }

    public static void put(String rowKey, String tableName, String columnFamiliName, String[] column, String[] value, boolean waiting) {
        hBaseService.bacthPut(rowKey, tableName, columnFamiliName, column, value, waiting);
    }

    /**
     * 获取多行数据
     *
     * @param tablename
     * @param rows
     * @return
     * @throws Exception
     */
    public static <T> Result[] getRows(String tablename, List<T> rows) throws Exception {
        return hBaseService.getRows(tablename, rows);
    }

    /**
     * 获取单条数据
     *
     * @param tablename
     * @param row
     * @return
     */
    public static Result getRow(String tablename, byte[] row) {
        return hBaseService.getRow(tablename, row);
    }

    /**
     * 获取具体value
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param columnName
     * @return
     */
    public static String getResultByColumn(String tableName, String rowKey, String familyName, String columnName) {
        return hBaseService.getResultByColumn(tableName, rowKey, familyName, columnName);
    }


}
