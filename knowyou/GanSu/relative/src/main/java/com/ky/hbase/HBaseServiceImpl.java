package com.ky.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;

/**
 * HBaseService Mutator 实现类
 */

class HBaseServiceImpl extends AbstractHBaseService {

    private static final Logger logger = LoggerFactory.getLogger(HBaseServiceImpl.class);

    private ExecutorService threadPool = Executors.newFixedThreadPool(20);

    @Override
    public void put(String tableName, Put put, boolean waiting) {
        batchPut(tableName, Collections.singletonList(put), waiting);

    }

    /**
     * 批量提交
     */
    @Override
    public void bacthPut(final String rowKey, final String tableName, final String columnFamiliName, final String[] column, final String[] value,
                         boolean waiting) {

        threadPool.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    HBaseUtil.put(rowKey, tableName, columnFamiliName, column, value);
                    logger.info("sucuss to hbase.....the current rowkey is :" + rowKey + " the tablename is: " + tableName);
                } catch (Exception e) {
                    logger.error("batchPut failed . .the rowkey is:" + rowKey + ",the tablename is :" + tableName + ",the root cause is :" + e.getMessage());
                }
            }
        });
        if (waiting) {
            try {
                threadPool.awaitTermination(3, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                logger.error("HBase put job thread pool await termination time out.", e);
            }
        }
    }

    /**
     * 获取具体的数据
     */
    @Override
    public String getResultByColumn(String tableName, String rowKey, String familyName, String columnName) {
        return super.getResultByColumn(tableName, rowKey, familyName, columnName);
    }

    @Override
    public <T> Result[] getRows(String tablename, List<T> rows) {
        return HBaseUtil.getRows(tablename, rows);
    }

    @Override
    public Result getRow(String tablename, byte[] row) {
        return HBaseUtil.getRow(tablename, row);
    }

    /**
     * 多线程异步提交
     */
    public void batchAsyncPut(final String tableName, final List<Put> puts, boolean waiting) {
        Future f = threadPool.submit(() -> {
            try {
                HBaseUtil.putByHTable(tableName, puts);
            } catch (Exception e) {
                logger.error("batchPut failed . ", e);
            }
        });

        if (waiting) {
            try {
                f.get();
            } catch (InterruptedException | ExecutionException e) {
                logger.error("多线程异步提交返回数据执行失败.", e);
            }
        }
    }

    /**
     * 创建表
     */
    @Override
    public void createTable(String tableName, String[] columnFamilies, boolean preBuildRegion) throws Exception {
        HBaseUtil.createTable(tableName, columnFamilies, preBuildRegion);
    }

}
