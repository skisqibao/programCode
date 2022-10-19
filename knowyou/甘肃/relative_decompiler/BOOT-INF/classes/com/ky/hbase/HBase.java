package com.ky.hbase;

import java.util.Arrays;
import java.util.List;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

public class HBase {
   private static HBaseService hBaseService = new HBaseServiceImpl();

   private HBase() {
   }

   public static void createTable(String tableName, String[] columnFamilies, boolean preBuildRegion) {
      try {
         hBaseService.createTable(tableName, columnFamilies, preBuildRegion);
      } catch (Exception var4) {
         System.out.println("create hbase table fail, the root cause is " + var4.getMessage());
      }

   }

   public static void put(String tableName, Put put, boolean waiting) {
      hBaseService.batchPut(tableName, Arrays.asList(put), waiting);
   }

   public static void put(String rowKey, String tableName, String columnFamiliName, String[] column, String[] value, boolean waiting) {
      hBaseService.bacthPut(rowKey, tableName, columnFamiliName, column, value, waiting);
   }

   public static <T> Result[] getRows(String tablename, List<T> rows) throws Exception {
      return hBaseService.getRows(tablename, rows);
   }

   public static Result getRow(String tablename, byte[] row) {
      return hBaseService.getRow(tablename, row);
   }

   public static String getResultByColumn(String tableName, String rowKey, String familyName, String columnName) {
      return hBaseService.getResultByColumn(tableName, rowKey, familyName, columnName);
   }
}
