package com.ky.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import java.util.List;

/**
 * HBase 服务接口类
 */
public interface HBaseService {

    /**
     * 创建表
     */
    public void createTable(String tableName, String[] columnFamilies, boolean preBuildRegion) throws Exception;

    /**
     * 写入数据
     */
    public void put(String tableName, Put put, boolean waiting);

    /**
     * 批量写入数据
     */
    public void batchPut(String tableName, final List<Put> puts, boolean waiting);

   /**
    * 批量写入
    * @param rowKey
    * @param tableName
    * @param columnFamiliName
    * @param column
    * @param value
    * @param waiting
    */
    public void bacthPut(String rowKey, String tableName, String columnFamiliName, String[] column, String[] value, boolean waiting);

    /**
     * @param tableName  表名称
     * @param rowKey     rk
     * @param familyName 列簇
     * @param columnName 具体column
     * @return
     */
    public String getResultByColumn(String tableName, String rowKey, String familyName, String columnName);

    <T> Result[] getRows(String tablename, List<T> rows);

    Result getRow(String tablename, byte[] row);
}
