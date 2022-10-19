package com.ky.hbase;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class HBaseServiceImpl extends AbstractHBaseService {
   private static final Logger logger = LoggerFactory.getLogger(HBaseServiceImpl.class);
   private ExecutorService threadPool = Executors.newFixedThreadPool(20);

   public void put(String tableName, Put put, boolean waiting) {
      this.batchPut(tableName, Collections.singletonList(put), waiting);
   }

   public void bacthPut(final String rowKey, final String tableName, final String columnFamiliName, final String[] column, final String[] value, boolean waiting) {
      this.threadPool.execute(new Runnable() {
         public void run() {
            try {
               HBaseUtil.put(rowKey, tableName, columnFamiliName, column, value);
               HBaseServiceImpl.logger.info("sucuss to hbase.....the current rowkey is :" + rowKey + " the tablename is: " + tableName);
            } catch (Exception var2) {
               HBaseServiceImpl.logger.error("batchPut failed . .the rowkey is:" + rowKey + ",the tablename is :" + tableName + ",the root cause is :" + var2.getMessage());
            }

         }
      });
      if (waiting) {
         try {
            this.threadPool.awaitTermination(3L, TimeUnit.SECONDS);
         } catch (InterruptedException var8) {
            logger.error("HBase put job thread pool await termination time out.", var8);
         }
      }

   }

   public String getResultByColumn(String tableName, String rowKey, String familyName, String columnName) {
      return super.getResultByColumn(tableName, rowKey, familyName, columnName);
   }

   public <T> Result[] getRows(String tablename, List<T> rows) {
      return HBaseUtil.getRows(tablename, rows);
   }

   public Result getRow(String tablename, byte[] row) {
      return HBaseUtil.getRow(tablename, row);
   }

   public void batchAsyncPut(final String tableName, final List<Put> puts, boolean waiting) {
      Future f = this.threadPool.submit(() -> {
         try {
            HBaseUtil.putByHTable(tableName, puts);
         } catch (Exception var3) {
            logger.error("batchPut failed . ", var3);
         }

      });
      if (waiting) {
         try {
            f.get();
         } catch (ExecutionException | InterruptedException var6) {
            logger.error("多线程异步提交返回数据执行失败.", var6);
         }
      }

   }

   public void createTable(String tableName, String[] columnFamilies, boolean preBuildRegion) throws Exception {
      HBaseUtil.createTable(tableName, columnFamilies, preBuildRegion);
   }
}
