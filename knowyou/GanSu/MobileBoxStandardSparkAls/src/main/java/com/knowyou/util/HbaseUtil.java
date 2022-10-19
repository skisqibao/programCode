package com.knowyou.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;

/**
 * @Author: Xnone
 * @Date: 2020/2/20 0020 11:26
 */
public class HbaseUtil {

    private static final Logger logger = LoggerFactory.getLogger(HbaseUtil.class);

    private static Configuration conf;
    private static Connection conn;

    static {
        try {
            if (conf == null) {
                conf = HBaseConfiguration.create();
                final Config config = Config.getInstance();
                final String zkHost = config.getProperty(UtilConstants.Public.HBASE_ZOOKEEPER_QUORUM);
                final String zkport = config.getProperty(UtilConstants.Public.HBASE_ZOOKEEPER_CLIENTPORT);
                final String znode = config.getProperty(UtilConstants.Public.HBASE_ZNODE_PARENT);
                conf.set(UtilConstants.Public.HBASE_ZOOKEEPER_QUORUM, zkHost);
                conf.set(UtilConstants.Public.HBASE_ZOOKEEPER_CLIENTPORT, zkport);
                conf.set(UtilConstants.Public.HBASE_ZNODE_PARENT, znode);
            }
        } catch (Exception e) {
            logger.error("HBase Config Initialization failure,配置文件信息获取异常");
            throw new RuntimeException(e);
        }
    }

    /**
     * 获得链接
     *
     * @return
     */
    public static synchronized Connection getConnection() {
        try {
            if (conn == null || conn.isClosed()) {
                conn = ConnectionFactory.createConnection(conf);

                logger.info("HBase链接已建立-------------------------------------------------");
            }
        } catch (IOException e) {
            logger.error("HBase 建立链接失败 ", e);
        }
        return conn;
    }

    public static void main(String[] args) {
        final String property = Config.getInstance()
                .getProperty("hbase.zookeeper.quorum", "xxxxxxx");
        System.out.println(property);
    }

    /**
     * 创建表
     *
     * @param tableName
     * @throws Exception
     */
    public static void createTable(String tableName, String[] columnFamilies, boolean preBuildRegion) throws Exception {
        if (preBuildRegion) {
            String[] s = new String[]{"1", "2", "3", "4", "5", "6", "7", "8", "9", "A", "B", "C", "D", "E", "F"};
            int partition = 16;
            byte[][] splitKeys = new byte[partition - 1][];
            for (int i = 1; i < partition; i++) {
                splitKeys[i - 1] = Bytes.toBytes(s[i - 1]);
            }
            createTable(tableName, columnFamilies, splitKeys);
        } else {
            createTable(tableName, columnFamilies);
        }
    }

    private static void createTable(String tableName, String[] cfs, byte[][] splitKeys) throws Exception {
        Connection conn = getConnection();
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        try {
            if (admin.tableExists(tableName)) {
                logger.warn("Table: {} is exists!", tableName);
                return;
            }
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            for (int i = 0; i < cfs.length; i++) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cfs[i]);
                hColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
                hColumnDescriptor.setMaxVersions(1);
                tableDesc.addFamily(hColumnDescriptor);
                hColumnDescriptor.setInMemory(true);
                //设置数据的保存时间----数据存5分钟
                hColumnDescriptor.setTimeToLive(90 * 24 * 60 * 60);
            }
            admin.createTable(tableDesc, splitKeys);
            logger.info("Table: {} create success!", tableName);
        } finally {
            admin.close();
            closeConnect(conn);
        }
    }

    private static void createTable(String tableName, String[] cfs) throws Exception {
        Connection conn = getConnection();
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        try {
            if (admin.tableExists(tableName)) {
                logger.warn("Table: {} is exists!", tableName);
                return;
            }
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            for (int i = 0; i < cfs.length; i++) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cfs[i]);
                hColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
                hColumnDescriptor.setMaxVersions(1);
                tableDesc.addFamily(hColumnDescriptor);
                hColumnDescriptor.setInMemory(true);
                //设置数据的保存时间----数据存5分钟
                hColumnDescriptor.setTimeToLive(5 * 60);
            }
            admin.createTable(tableDesc);
            logger.info("Table: {} create success!", tableName);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            admin.close();
            closeConnect(conn);
        }
    }

    /**
     * 删除表
     *
     * @param tablename
     * @throws IOException
     */
    public static void deleteTable(String tablename) throws IOException {
        Connection conn = getConnection();
        HBaseAdmin admin = (HBaseAdmin) conn.getAdmin();
        try {
            if (!admin.tableExists(tablename)) {
                logger.warn("Table: {} is not exists!", tablename);
                return;
            }
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
            logger.info("Table: {} delete success!", tablename);
        } finally {
            admin.close();
            closeConnect(conn);
        }
    }

    /**
     * 获取 Table
     *
     * @param tableName 表名
     * @return
     * @throws IOException
     */
    public static Table getTable(String tableName) {
        try {
            return getConnection().getTable(TableName.valueOf(tableName));
        } catch (Exception e) {
            logger.error("Obtain Table failure !", e);
        }
        return null;
    }

    /**
     * 异步的往表里面添加数据
     *
     * @param tablename
     * @param rowKey
     * @param columnFamiliName
     * @param column
     * @param value
     * @return
     * @throws Exception
     */
    //String rowKey, String tableName, String columnFamiliName, String[] column,String[] value
/*    public static long put(String rowKey, String tablename, String columnFamiliName, String[] column, String[] value)
            throws Exception {
        long currentTime = System.currentTimeMillis();
        Table htable = getConnection().getTable(TableName.valueOf(tablename));
        Put put = new Put(Bytes.toBytes(rowKey));
        HColumnDescriptor[] columnFamilies = htable.getTableDescriptor().getColumnFamilies();
        for (int i = 0; i < columnFamilies.length; i++) {
            String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
            if (familyName.equals(columnFamiliName)) {
                for (int j = 0; j < column.length; j++) {
                    put.addColumn(Bytes.toBytes(columnFamiliName), Bytes.toBytes(column[j]), Bytes.toBytes(value[j]));
                }
            }
        }
        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    logger.error("Failed to sent put " + e.getRow(i) + ".");
                }
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tablename)).listener(listener);
        params.writeBufferSize(5 * 1024 * 1024);
        final BufferedMutator mutator = conn.getBufferedMutator(params);
        try {
            final List<Put> puts = Arrays.asList(put);
            mutator.mutate(puts);
            mutator.flush();
        } finally {
            mutator.close();
            closeConnect(conn);
        }
        return System.currentTimeMillis() - currentTime;
    }*/

    public static void put(String rowKey, String tablename, String columnFamiliName, String[] column, String[] value)
            throws Exception {
        Table htable = getConnection().getTable(TableName.valueOf(tablename));

//        logger.info(htable.getName().getNameAsString() + "已连接。。。" + "命名空间：" + htable.getName().getNamespaceAsString()+"---------------------------");

        Put put = new Put(Bytes.toBytes(rowKey));
        for (int i = 0; i < column.length; i++) {
            put.addColumn(columnFamiliName.getBytes(), column[i].getBytes(), value[i].getBytes());

//            logger.info("已添加到Put对象信息：列簇--{}, 列名--{}, 属性值--{}-------------------------------", columnFamiliName, column[i], value[i] );
        }

//        logger.info("Put已添加完成---------------");

        final BufferedMutator.ExceptionListener listener = (e, mutator) -> {
            for (int i = 0; i < e.getNumExceptions(); i++) {
                logger.error("Failed to sent put " + e.getRow(i) + ".");
            }
        };

//        logger.info("ExceptionListener已创建");

        BufferedMutatorParams params = new BufferedMutatorParams(htable.getName()).listener(listener);
        params.writeBufferSize(5 * 1024 * 1024);

//        logger.info("Buffer已创建，设置写缓冲大小");

        try (BufferedMutator mutator = conn.getBufferedMutator(params)) {

//            logger.info("创建流对象成功--------------------------------");

            final List<Put> puts = singletonList(put);

//            logger.info("Put列表已生成-----------------");

            mutator.mutate(puts);
            mutator.flush();
//            logger.info("the rk " + rowKey + " send to table " + tablename);
        } finally {
            closeConnect(conn);
        }
        System.currentTimeMillis();
    }

    /**
     * 异步往指定表添加数据
     *
     * @param tablename 表名
     * @param put       需要添加的数据
     * @return long 返回执行时间
     * @throws IOException
     */
    public static long put(String tablename, Put put) throws Exception {
        return put(tablename, (Put) Arrays.asList(put));
    }

    /**
     * 往指定表添加数据
     *
     * @param tablename 表名
     * @param puts      需要添加的数据
     * @return long 返回执行时间
     * @throws IOException
     */
    public static long putByHTable(String tablename, List<?> puts) throws Exception {
        long currentTime = System.currentTimeMillis();
        Connection conn = getConnection();
        HTable htable = (HTable) conn.getTable(TableName.valueOf(tablename));
        htable.setAutoFlushTo(false);
        htable.setWriteBufferSize(5 * 1024 * 1024);
        try {
            htable.put((List<Put>) puts);
            htable.flushCommits();
        } finally {
            htable.close();
            closeConnect(conn);
        }
        return System.currentTimeMillis() - currentTime;
    }

    /**
     * 删除单条数据
     *
     * @param tablename
     * @param row
     * @throws IOException
     */
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

    /**
     * 删除多行数据
     *
     * @param tablename
     * @param rows
     * @throws IOException
     */
    public static void delete(String tablename, String[] rows) throws IOException {
        Table table = getTable(tablename);
        if (table != null) {
            try {
                List<Delete> list = new ArrayList<Delete>();
                for (String row : rows) {
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

    /**
     * 关闭连接
     *
     * @throws IOException
     */
    public static void closeConnect(Connection conn) {
        if (null != conn) {
            try {
                // conn.close();
            } catch (Exception e) {
                logger.error("closeConnect failure !", e);
            }
        }
    }

    /**
     * 获取单条数据
     *
     * @param tablename
     * @param row
     * @return
     * @throws IOException
     */
    public static Result getRow(String tablename, byte[] row) {
        Table table = getTable(tablename);
        Result rs = null;
        if (table != null) {
            try {
                Get g = new Get(row);
                rs = table.get(g);
            } catch (IOException e) {
                logger.error("getRow failure !", e);
            } finally {
                try {
                    table.close();
                } catch (IOException e) {
                    logger.error("getRow failure !", e);
                }
            }
        }
        return rs;
    }

    /**
     * 获取多行数据
     *
     * @param tablename
     * @param rows
     * @return
     * @throws Exception
     */
    public static <T> Result[] getRows(String tablename, List<T> rows) {
        Table table = getTable(tablename);
        List<Get> gets = null;
        Result[] results = null;
        try {
            if (table != null) {
                gets = new ArrayList<Get>();
                for (T row : rows) {
                    if (row != null) {
                        gets.add(new Get(Bytes.toBytes(String.valueOf(row))));
                    } else {
                        throw new RuntimeException("hbase have no data");
                    }
                }
            }
            if (gets.size() > 0) {
                results = table.get(gets);
            }
        } catch (IOException e) {
            logger.error("getRows failure !", e);
        } finally {
            try {
                table.close();
            } catch (IOException e) {
                logger.error("table.close() failure !", e);
            }
        }
        return results;
    }

    /**
     * 扫描整张表
     *
     * @param tablename
     * @return
     * @throws IOException
     */
    public static ResultScanner get(String tablename) {
        Table table = getTable(tablename);
        ResultScanner results = null;
        if (table != null) {
            try {
                Scan scan = new Scan();
                scan.setCaching(1000);
                results = table.getScanner(scan);
            } catch (IOException e) {
                logger.error("getResultScanner failure !", e);
            } finally {
                try {
                    table.close();
                } catch (IOException e) {
                    logger.error("table.close() failure !", e);
                }
            }
        }
        return results;
    }

    public static void addData(String rowKey, String tableName, String columnFamiliName, String[] column,
                               String[] value) throws IOException {
        Table htable = null;
        try {
            htable = getConnection().getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            // 获取表
            HColumnDescriptor[] columnFamilies = htable.getTableDescriptor() // 获取所有的列族
                    .getColumnFamilies();

            for (int i = 0; i < columnFamilies.length; i++) {
                String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
                if (familyName.equals(columnFamiliName)) {
                    for (int j = 0; j < column.length; j++) {
                        put.add(Bytes.toBytes(columnFamiliName), Bytes.toBytes(column[j]), Bytes.toBytes(value[j]));
                    }
                }
            }
            htable.put(put);
            htable.close();
//			System.out.println("add data Success! the rk is :"+rowKey);
            logger.info("add data Success! the rk is :" + rowKey);
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            if (htable != null) {
                try {
                    htable.close();
                } catch (IOException e) {
                    logger.error(e.getMessage());
                }
            }

        }
    }

    public static void addDataAndCloseConn(String rowKey, String tableName, String columnFamiliName, String[] column,
                                           String[] value) throws IOException {
        Table htable = null;
        try {
            htable = getConnection().getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            // 获取表
            HColumnDescriptor[] columnFamilies = htable.getTableDescriptor() // 获取所有的列族
                    .getColumnFamilies();

            for (int i = 0; i < columnFamilies.length; i++) {
                String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
                if (familyName.equals(columnFamiliName)) {
                    for (int j = 0; j < column.length; j++) {
                        put.add(Bytes.toBytes(columnFamiliName), Bytes.toBytes(column[j]), Bytes.toBytes(value[j]));
                    }
                }
            }
            htable.put(put);
            htable.close();
            System.out.println("add data Success!");
        } catch (Exception e) {
            logger.error(e.getMessage());
        } finally {
            if (htable != null) {
                try {
                    htable.close();
                    if (conn != null) {
                        conn.close();
                    }
                } catch (IOException e) {
                    logger.error(e.getMessage());
                }
            }

        }
    }

    /*
     * 查询表中的某一列
     *
     * @tableName 表名
     *
     * @rowKey rowKey
     */
    public static String getResultByColumn(String tableName, String rowKey, String familyName, String columnName) {
        Table htable = null;
        try {
            htable = getConnection().getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列
            Result result = htable.get(get);
            String value = "";
            if (result != null && !result.isEmpty()) {
                for (KeyValue kv : result.list()) {
                    value = Bytes.toString(kv.getValue());
                }
            }
            return value;
        } catch (Exception e) {
            logger.error(e.getMessage());
            return "";
        } finally {
            if (htable != null) {
                try {
                    htable.close();
                } catch (IOException e) {
                    logger.error(e.getMessage());
                }
            }

        }
    }

    public static String getResultByColumnAndCloseConn(String tableName, String rowKey, String familyName,
                                                       String columnName) {
        Table htable = null;
        try {
            htable = getConnection().getTable(TableName.valueOf(tableName));

            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列
            Result result = htable.get(get);
            String value = "";
            if (result != null && !result.isEmpty()) {
                for (KeyValue kv : result.list()) {
                    value = Bytes.toString(kv.getValue());
                }
            }
            return value;
        } catch (Exception e) {
            logger.error(e.getMessage());
            return "";
        } finally {
            if (htable != null) {
                try {
                    htable.close();
                    if (conn != null) {
                        conn.close();
                    }
                } catch (IOException e) {
                    logger.error(e.getMessage());
                }
            }

        }
    }

    /**
     * 测试输出结果
     */
    public static void formatRow(KeyValue[] rs) {
        for (KeyValue kv : rs) {
            System.out.println(" column family  :  " + Bytes.toString(kv.getFamily()));
            System.out.println(" column   :  " + Bytes.toString(kv.getQualifier()));
            System.out.println(" value   :  " + Bytes.toString(kv.getValue()));
            System.out.println(" timestamp   :  " + String.valueOf(kv.getTimestamp()));
            System.out.println("--------------------");
        }
    }


}
