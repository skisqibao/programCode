package com.ky.hbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * HBase 服务抽象类
 */

public abstract class AbstractHBaseService implements HBaseService {

    private static final Logger logger = LoggerFactory.getLogger(AbstractHBaseService.class);

    @Override
    public void createTable(String tableName, String[] columnFamilies, boolean preBuildRegion) throws Exception {

    }

    @Override
    public void put(String tableName, Put put, boolean waiting) {
    }

    @Override
    public void batchPut(final String tableName, final List<Put> puts, boolean waiting) {
    }

    @Override
    public void bacthPut(String rowKey, String tableName, String columnFamiliName, String[] column, String[] value, boolean waiting) {

    }

    @Override
    public String getResultByColumn(String tableName, String rowKey, String familyName, String columnName) {
        return HBaseUtil.getResultByColumn(tableName, rowKey, familyName, columnName);
    }

    @Override
    public <T> Result[] getRows(String tablename, List<T> rows) {
        return null;
    }

    @Override
    public Result getRow(String tablename, byte[] row) {
        return null;
    }

}
