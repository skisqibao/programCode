package com.ky.hbase;

import java.util.List;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

public interface HBaseService {
   void createTable(String tableName, String[] columnFamilies, boolean preBuildRegion) throws Exception;

   void put(String tableName, Put put, boolean waiting);

   void batchPut(String tableName, final List<Put> puts, boolean waiting);

   void bacthPut(String rowKey, String tableName, String columnFamiliName, String[] column, String[] value, boolean waiting);

   String getResultByColumn(String tableName, String rowKey, String familyName, String columnName);

   <T> Result[] getRows(String tablename, List<T> rows);

   Result getRow(String tablename, byte[] row);
}
