package com.ky.hbase;

import java.util.List;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractHBaseService implements HBaseService {
   private static final Logger logger = LoggerFactory.getLogger(AbstractHBaseService.class);

   public void createTable(String tableName, String[] columnFamilies, boolean preBuildRegion) throws Exception {
   }

   public void put(String tableName, Put put, boolean waiting) {
   }

   public void batchPut(final String tableName, final List<Put> puts, boolean waiting) {
   }

   public void bacthPut(String rowKey, String tableName, String columnFamiliName, String[] column, String[] value, boolean waiting) {
   }

   public String getResultByColumn(String tableName, String rowKey, String familyName, String columnName) {
      return HBaseUtil.getResultByColumn(tableName, rowKey, familyName, columnName);
   }

   public <T> Result[] getRows(String tablename, List<T> rows) {
      return null;
   }

   public Result getRow(String tablename, byte[] row) {
      return null;
   }
}
