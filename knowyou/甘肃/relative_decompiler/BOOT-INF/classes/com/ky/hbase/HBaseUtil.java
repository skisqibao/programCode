package com.ky.hbase;

import com.ky.common.CommonUtil;
import com.ky.common.PropertyUtil;
import com.ky.entity.vo.MediaInfo;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.BufferedMutator.ExceptionListener;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HBaseUtil {
   private static final Logger logger = LoggerFactory.getLogger(HBaseUtil.class);
   private static Configuration conf;
   private static Connection conn;

   public static void addData(String rowKey, String tableName, String columnFamiliName, String[] column, String[] value) throws IOException {
      Table htable = null;

      try {
         htable = getConnection().getTable(TableName.valueOf(tableName));
         Put put = new Put(Bytes.toBytes(rowKey));
         HColumnDescriptor[] columnFamilies = htable.getTableDescriptor().getColumnFamilies();
         HColumnDescriptor[] var8 = columnFamilies;
         int var9 = columnFamilies.length;

         for(int var10 = 0; var10 < var9; ++var10) {
            HColumnDescriptor columnFamily = var8[var10];
            String familyName = columnFamily.getNameAsString();
            if (familyName.equals(columnFamiliName)) {
               for(int j = 0; j < column.length; ++j) {
                  put.add(Bytes.toBytes(columnFamiliName), Bytes.toBytes(column[j]), Bytes.toBytes(value[j]));
               }
            }
         }

         htable.put(put);
         htable.close();
         logger.info("add data Success! the rk is :" + rowKey);
      } catch (Exception var22) {
         logger.error(var22.getMessage());
      } finally {
         if (htable != null) {
            try {
               htable.close();
            } catch (IOException var21) {
               logger.error(var21.getMessage());
            }
         }

      }

   }

   public static void addDataAndCloseConn(String rowKey, String tableName, String columnFamiliName, String[] column, String[] value) throws IOException {
      Table htable = null;

      try {
         htable = getConnection().getTable(TableName.valueOf(tableName));
         Put put = new Put(Bytes.toBytes(rowKey));
         HColumnDescriptor[] columnFamilies = htable.getTableDescriptor().getColumnFamilies();
         HColumnDescriptor[] var8 = columnFamilies;
         int var9 = columnFamilies.length;

         for(int var10 = 0; var10 < var9; ++var10) {
            HColumnDescriptor columnFamily = var8[var10];
            String familyName = columnFamily.getNameAsString();
            if (familyName.equals(columnFamiliName)) {
               for(int j = 0; j < column.length; ++j) {
                  put.add(Bytes.toBytes(columnFamiliName), Bytes.toBytes(column[j]), Bytes.toBytes(value[j]));
               }
            }
         }

         htable.put(put);
         htable.close();
         System.out.println("add data Success!");
      } catch (Exception var22) {
         logger.error(var22.getMessage());
      } finally {
         if (htable != null) {
            try {
               htable.close();
               if (conn != null) {
                  conn.close();
               }
            } catch (IOException var21) {
               logger.error(var21.getMessage());
            }
         }

      }

   }

   public static void createTable(String tableName, String[] columnFamilies, boolean preBuildRegion) throws Exception {
      if (preBuildRegion) {
         String[] s = new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F"};
         int partition = 16;
         byte[][] splitKeys = new byte[partition - 1][];

         for(int i = 1; i < partition; ++i) {
            splitKeys[i - 1] = Bytes.toBytes(s[i - 1]);
         }

         createTable(tableName, columnFamilies, splitKeys);
      } else {
         createTable(tableName, columnFamilies);
      }

   }

   private static void createTable(String tableName, String[] cfs, byte[][] splitKeys) throws Exception {
      Connection conn = getConnection();

      try {
         HBaseAdmin admin = (HBaseAdmin)conn.getAdmin();
         Throwable var5 = null;

         try {
            if (!admin.tableExists(tableName)) {
               HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
               String[] var7 = cfs;
               int var8 = cfs.length;

               for(int var9 = 0; var9 < var8; ++var9) {
                  String cf = var7[var9];
                  HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
                  hColumnDescriptor.setCompressionType(Algorithm.SNAPPY);
                  hColumnDescriptor.setMaxVersions(1);
                  tableDesc.addFamily(hColumnDescriptor);
                  hColumnDescriptor.setInMemory(true);
               }

               admin.createTable(tableDesc, splitKeys);
               logger.info("Table: {} create success!", tableName);
               return;
            }

            logger.warn("Table: {} is exists!", tableName);
         } catch (Throwable var28) {
            var5 = var28;
            throw var28;
         } finally {
            if (admin != null) {
               if (var5 != null) {
                  try {
                     admin.close();
                  } catch (Throwable var27) {
                     var5.addSuppressed(var27);
                  }
               } else {
                  admin.close();
               }
            }

         }
      } finally {
         closeConnect(conn);
      }

   }

   private static void createTable(String tableName, String[] cfs) throws Exception {
      Connection conn = getConnection();

      try {
         HBaseAdmin admin = (HBaseAdmin)conn.getAdmin();
         Throwable var4 = null;

         try {
            if (admin.tableExists(tableName)) {
               logger.warn("Table: {} is exists!", tableName);
               return;
            }

            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            String[] var6 = cfs;
            int var7 = cfs.length;

            for(int var8 = 0; var8 < var7; ++var8) {
               String cf = var6[var8];
               HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
               hColumnDescriptor.setCompressionType(Algorithm.SNAPPY);
               hColumnDescriptor.setMaxVersions(1);
               tableDesc.addFamily(hColumnDescriptor);
               hColumnDescriptor.setInMemory(true);
            }

            admin.createTable(tableDesc);
            logger.info("Table: {} create success!", tableName);
         } catch (Throwable var29) {
            var4 = var29;
            throw var29;
         } finally {
            if (admin != null) {
               if (var4 != null) {
                  try {
                     admin.close();
                  } catch (Throwable var28) {
                     var4.addSuppressed(var28);
                  }
               } else {
                  admin.close();
               }
            }

         }
      } catch (Exception var31) {
         var31.printStackTrace();
      } finally {
         closeConnect(conn);
      }

   }

   public static synchronized Connection getConnection() {
      try {
         if (conn == null || conn.isClosed()) {
            conn = ConnectionFactory.createConnection(conf);
         }
      } catch (IOException var1) {
         logger.error("HBase 建立链接失败 ", var1);
      }

      return conn;
   }

   public static void closeConnect(Connection conn) {
      if (null != conn) {
      }

   }

   public static void delete(String tablename, String row) throws IOException {
      Table table = getTable(tablename);
      if (table != null) {
         try {
            Delete d = new Delete(row.getBytes());
            table.delete(d);
         } finally {
            table.close();
         }
      }

   }

   public static Table getTable(String tableName) {
      try {
         return getConnection().getTable(TableName.valueOf(tableName));
      } catch (Exception var2) {
         logger.error("Obtain Table failure !", var2);
         return null;
      }
   }

   public static void delete(String tablename, String[] rows) throws IOException {
      Table table = getTable(tablename);
      if (table != null) {
         try {
            List<Delete> list = new ArrayList();
            String[] var4 = rows;
            int var5 = rows.length;

            for(int var6 = 0; var6 < var5; ++var6) {
               String row = var4[var6];
               Delete d = new Delete(row.getBytes());
               list.add(d);
            }

            if (list.size() > 0) {
               table.delete(list);
            }
         } finally {
            table.close();
         }
      }

   }

   public static void deleteTable(String tablename) throws IOException {
      Connection conn = getConnection();

      try {
         HBaseAdmin admin = (HBaseAdmin)conn.getAdmin();
         Throwable var3 = null;

         try {
            if (admin.tableExists(tablename)) {
               admin.disableTable(tablename);
               admin.deleteTable(tablename);
               logger.info("Table: {} delete success!", tablename);
               return;
            }

            logger.warn("Table: {} is not exists!", tablename);
         } catch (Throwable var21) {
            var3 = var21;
            throw var21;
         } finally {
            if (admin != null) {
               if (var3 != null) {
                  try {
                     admin.close();
                  } catch (Throwable var20) {
                     var3.addSuppressed(var20);
                  }
               } else {
                  admin.close();
               }
            }

         }
      } finally {
         closeConnect(conn);
      }

   }

   public static void formatRow(KeyValue[] rs) {
      KeyValue[] var1 = rs;
      int var2 = rs.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         KeyValue kv = var1[var3];
         System.out.println(" column family  :  " + Bytes.toString(kv.getFamily()));
         System.out.println(" column   :  " + Bytes.toString(kv.getQualifier()));
         System.out.println(" value   :  " + Bytes.toString(kv.getValue()));
         System.out.println(" timestamp   :  " + String.valueOf(kv.getTimestamp()));
         System.out.println("--------------------");
      }

   }

   public static ResultScanner get(String tablename) {
      Table table = getTable(tablename);
      ResultScanner results = null;
      if (table != null) {
         try {
            Scan scan = new Scan();
            scan.setCaching(1000);
            results = table.getScanner(scan);
         } catch (IOException var12) {
            logger.error("getResultScanner failure !", var12);
         } finally {
            try {
               table.close();
            } catch (IOException var11) {
               logger.error("table.close() failure !", var11);
            }

         }
      }

      return results;
   }

   public static List<String> getList(String tableName) {
      Table table = getTable(tableName);
      ArrayList<String> list = new ArrayList();
      if (table != null) {
         try {
            Scan scan = new Scan();
            scan.setCaching(1000);
            ResultScanner results = table.getScanner(scan);
            Iterator var5 = results.iterator();

            while(var5.hasNext()) {
               Result result = (Result)var5.next();
               List<Cell> cells = result.listCells();
               Iterator var8 = cells.iterator();

               while(var8.hasNext()) {
                  Cell cell = (Cell)var8.next();
                  String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                  list.add(row);
               }
            }
         } catch (IOException var19) {
            logger.error("getResultScanner failure !", var19);
         } finally {
            try {
               table.close();
            } catch (IOException var18) {
               logger.error("table.close() failure !", var18);
            }

         }
      }

      return list;
   }

   public static String getResultByColumn(String tableName, String rowKey, String familyName, String columnName) {
      Table htable = null;

      String var6;
      try {
         htable = getConnection().getTable(TableName.valueOf(tableName));
         Get get = new Get(Bytes.toBytes(rowKey));
         get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
         Result result = htable.get(get);
         String value = "";
         KeyValue kv;
         if (result != null && !result.isEmpty()) {
            for(Iterator var8 = result.list().iterator(); var8.hasNext(); value = Bytes.toString(kv.getValue())) {
               kv = (KeyValue)var8.next();
            }
         }

         String var21 = value;
         return var21;
      } catch (Exception var18) {
         logger.error(var18.getMessage());
         var6 = "";
      } finally {
         if (htable != null) {
            try {
               htable.close();
            } catch (IOException var17) {
               logger.error(var17.getMessage());
            }
         }

      }

      return var6;
   }

   public static String getResultByColumnAndCloseConn(String tableName, String rowKey, String familyName, String columnName) {
      Table htable = null;

      String var6;
      try {
         htable = getConnection().getTable(TableName.valueOf(tableName));
         Get get = new Get(Bytes.toBytes(rowKey));
         get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
         Result result = htable.get(get);
         String value = "";
         KeyValue kv;
         if (result != null && !result.isEmpty()) {
            for(Iterator var8 = result.list().iterator(); var8.hasNext(); value = Bytes.toString(kv.getValue())) {
               kv = (KeyValue)var8.next();
            }
         }

         String var21 = value;
         return var21;
      } catch (Exception var18) {
         logger.error(var18.getMessage());
         var6 = "";
      } finally {
         if (htable != null) {
            try {
               htable.close();
               if (conn != null) {
                  conn.close();
               }
            } catch (IOException var17) {
               logger.error(var17.getMessage());
            }
         }

      }

      return var6;
   }

   public static Result getRow(String tablename, byte[] row) {
      Table table = getTable(tablename);
      Result rs = null;
      if (table != null) {
         try {
            Get g = new Get(row);
            rs = table.get(g);
         } catch (IOException var13) {
            logger.error("getRow failure !", var13);
         } finally {
            try {
               table.close();
            } catch (IOException var12) {
               logger.error("getRow failure !", var12);
            }

         }
      }

      return rs;
   }

   public static <T> Result[] getRows(String tablename, List<T> rows) {
      Table table = getTable(tablename);
      List<Get> gets = null;
      Result[] results = null;

      try {
         if (table != null) {
            gets = new ArrayList();
            Iterator var5 = rows.iterator();

            while(var5.hasNext()) {
               T row = var5.next();
               if (row == null) {
                  throw new RuntimeException("hbase have no data");
               }

               gets.add(new Get(Bytes.toBytes(String.valueOf(row))));
            }
         }

         if (gets != null && gets.size() > 0) {
            results = table.get(gets);
         }
      } catch (IOException var15) {
         logger.error("getRows failure !", var15);
      } finally {
         try {
            ((Table)Objects.requireNonNull(table)).close();
         } catch (IOException var14) {
            logger.error("table.close() failure !", var14);
         }

      }

      return results;
   }

   public static void put(String rowKey, String tablename, String columnFamiliName, String[] column, String[] value) throws Exception {
      long currentTime = System.currentTimeMillis();
      Table htable = getConnection().getTable(TableName.valueOf(tablename));
      Put put = new Put(Bytes.toBytes(rowKey));
      HColumnDescriptor[] columnFamilies = htable.getTableDescriptor().getColumnFamilies();
      HColumnDescriptor[] var10 = columnFamilies;
      int var11 = columnFamilies.length;

      for(int var12 = 0; var12 < var11; ++var12) {
         HColumnDescriptor columnFamily = var10[var12];
         String familyName = columnFamily.getNameAsString();
         if (familyName.equals(columnFamiliName)) {
            for(int j = 0; j < column.length; ++j) {
               put.addColumn(Bytes.toBytes(columnFamiliName), Bytes.toBytes(column[j]), Bytes.toBytes(value[j]));
            }
         }
      }

      ExceptionListener listener = new ExceptionListener() {
         public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
            for(int i = 0; i < e.getNumExceptions(); ++i) {
               HBaseUtil.logger.error("Failed to sent put " + e.getRow(i) + ".");
            }

         }
      };
      BufferedMutatorParams params = (new BufferedMutatorParams(TableName.valueOf(tablename))).listener(listener);
      params.writeBufferSize(5242880L);

      try {
         BufferedMutator mutator = conn.getBufferedMutator(params);
         Throwable var36 = null;

         try {
            List<Put> puts = Collections.singletonList(put);
            mutator.mutate(puts);
            mutator.flush();
         } catch (Throwable var30) {
            var36 = var30;
            throw var30;
         } finally {
            if (mutator != null) {
               if (var36 != null) {
                  try {
                     mutator.close();
                  } catch (Throwable var29) {
                     var36.addSuppressed(var29);
                  }
               } else {
                  mutator.close();
               }
            }

         }
      } finally {
         closeConnect(conn);
      }

      System.currentTimeMillis();
   }

   public static long put(String tablename, Put put) throws Exception {
      return put(tablename, (Put)Collections.singletonList(put));
   }

   public static void putByHTable(String tablename, List<?> puts) throws Exception {
      Connection conn = getConnection();
      HTable htable = (HTable)conn.getTable(TableName.valueOf(tablename));
      htable.setAutoFlushTo(false);
      htable.setWriteBufferSize(5242880L);

      try {
         htable.put(puts);
         htable.flushCommits();
      } finally {
         htable.close();
         closeConnect(conn);
      }

      System.currentTimeMillis();
   }

   public static List<MediaInfo> getRecommendList(String table) {
      List<MediaInfo> mediaInfoArrayList = new ArrayList();
      ResultScanner results = get(table);
      PropertyUtil prop = PropertyUtil.getInstance();
      results.forEach((result) -> {
         try {
            MediaInfo mediaInfo = new MediaInfo();
            String videoId = Bytes.toString(result.getRow());
            mediaInfo.setVideoId(videoId);
            Cell[] var5 = result.rawCells();
            int var6 = var5.length;

            for(int var7 = 0; var7 < var6; ++var7) {
               Cell cell = var5[var7];
               String cpType;
               if (prop.getString("videoName", "videoName").equalsIgnoreCase(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                  cpType = Bytes.toString(CellUtil.cloneValue(cell));
                  mediaInfo.setVideoName(judgeValue(cpType));
               }

               if (prop.getString("directName", "directName").equalsIgnoreCase(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                  cpType = Bytes.toString(CellUtil.cloneValue(cell));
                  mediaInfo.setDriectorList(CommonUtil.getFilmerList(judgeValue(cpType)));
               }

               if (prop.getString("actorName", "actorName").equalsIgnoreCase(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                  cpType = Bytes.toString(CellUtil.cloneValue(cell));
                  mediaInfo.setActorList(CommonUtil.getFilmerList(cpType));
               }

               if (prop.getString("videoPlot", "videoPlot").equalsIgnoreCase(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                  cpType = Bytes.toString(CellUtil.cloneValue(cell));
                  mediaInfo.setPlotList(CommonUtil.getFilmerList(judgeValue(cpType)));
               }

               if (prop.getString("videoRegion", "videoRegion").equalsIgnoreCase(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                  cpType = Bytes.toString(CellUtil.cloneValue(cell));
                  mediaInfo.setRegions(CommonUtil.getCleanRegion(judgeValue(cpType)));
               }

               if (prop.getString("videoScore", "videoScore").equalsIgnoreCase(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                  cpType = Bytes.toString(CellUtil.cloneValue(cell));
                  if (StringUtils.isEmpty(cpType)) {
                     cpType = "0";
                  }

                  mediaInfo.setVideoScore(cpType);
               }

               if (prop.getString("videoType", "videoType").equalsIgnoreCase(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                  cpType = Bytes.toString(CellUtil.cloneValue(cell));
                  mediaInfo.setVideoType(judgeValue(cpType));
               }

               if (prop.getString("cpType", "cpType").equalsIgnoreCase(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                  cpType = Bytes.toString(CellUtil.cloneValue(cell));
                  mediaInfo.setCpType(judgeValue(cpType));
               }
            }

            mediaInfoArrayList.add(mediaInfo);
         } catch (Exception var10) {
            var10.printStackTrace();
         }

      });
      return mediaInfoArrayList;
   }

   private static String judgeValue(String value) {
      return StringUtils.isEmpty(value) ? "other" : value;
   }

   static {
      try {
         if (conf == null) {
            conf = HBaseConfiguration.create();
            PropertyUtil property = PropertyUtil.getInstance();
            String zkHost = property.getProperty("hbase.zookeeper.quorum", "10.1.11.108,10.1.11.110,10.1.11.111");
            String zkport = property.getProperty("hbase.zooKeeper.property.clientport", "2181");
            String znode = property.getProperty("zookeeper.znode.parent", "/hbase-unsecure");
            conf.set("hbase.zookeeper.quorum", zkHost);
            conf.set("hbase.zooKeeper.property.clientport", zkport);
            conf.set("zookeeper.znode.parent", znode);
         }

      } catch (Exception var4) {
         logger.error("HBase Configuration Initialization failure !");
         throw new RuntimeException(var4);
      }
   }
}
