package com.ky.hbase;

import com.ky.common.CommonUtil;
import com.ky.common.PropertyUtil;
import com.ky.entity.vo.MediaInfo;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * HBase 工具类
 */
public class HBaseUtil {

    private static final Logger logger = LoggerFactory.getLogger(HBaseUtil.class);

    private static Configuration conf;
    private static Connection conn;

    static {
        try {
            if (conf == null) {
                conf = HBaseConfiguration.create();
                PropertyUtil property = PropertyUtil.getInstance();
                final String zkHost = property.getProperty("hbase.zookeeper.quorum",
                        "10.1.11.108,10.1.11.110,10.1.11.111");
                final String zkport = property.getProperty("hbase.zooKeeper.property.clientport", "2181");
                final String znode = property.getProperty("zookeeper.znode.parent", "/hbase-unsecure");
                conf.set("hbase.zookeeper.quorum", zkHost);
                conf.set("hbase.zooKeeper.property.clientport", zkport);
                conf.set("zookeeper.znode.parent", znode);
            }
        } catch (Exception e) {
            logger.error("HBase Configuration Initialization failure !");
            throw new RuntimeException(e);
        }
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

            for (HColumnDescriptor columnFamily : columnFamilies) {
                String familyName = columnFamily.getNameAsString(); // 获取列族名
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

            for (HColumnDescriptor columnFamily : columnFamilies) {
                String familyName = columnFamily.getNameAsString(); // 获取列族名
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
        try (HBaseAdmin admin = (HBaseAdmin) conn.getAdmin()) {
            if (admin.tableExists(tableName)) {
                logger.warn("Table: {} is exists!", tableName);
                return;
            }
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf : cfs) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
                hColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
                hColumnDescriptor.setMaxVersions(1);
                tableDesc.addFamily(hColumnDescriptor);
                hColumnDescriptor.setInMemory(true);
                //设置数据的保存时间----数据存90天
//                hColumnDescriptor.setTimeToLive(90 * 24 * 60 * 60);
            }
            admin.createTable(tableDesc, splitKeys);
            logger.info("Table: {} create success!", tableName);
        } finally {
            closeConnect(conn);
        }
    }

    private static void createTable(String tableName, String[] cfs) throws Exception {
        Connection conn = getConnection();
        try (HBaseAdmin admin = (HBaseAdmin) conn.getAdmin()) {
            if (admin.tableExists(tableName)) {
                logger.warn("Table: {} is exists!", tableName);
                return;
            }
            HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tableName));
            for (String cf : cfs) {
                HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(cf);
                hColumnDescriptor.setCompressionType(Compression.Algorithm.SNAPPY);
                hColumnDescriptor.setMaxVersions(1);
                tableDesc.addFamily(hColumnDescriptor);
                hColumnDescriptor.setInMemory(true);
                //设置数据的保存时间----数据存5分钟
//                hColumnDescriptor.setTimeToLive(5 * 60);
            }
            admin.createTable(tableDesc);
            logger.info("Table: {} create success!", tableName);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            closeConnect(conn);
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
            }
        } catch (IOException e) {
            logger.error("HBase 建立链接失败 ", e);
        }
        return conn;
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
     * 删除表
     *
     * @param tablename
     * @throws IOException
     */
    public static void deleteTable(String tablename) throws IOException {
        Connection conn = getConnection();
        try (HBaseAdmin admin = (HBaseAdmin) conn.getAdmin()) {
            if (!admin.tableExists(tablename)) {
                logger.warn("Table: {} is not exists!", tablename);
                return;
            }
            admin.disableTable(tablename);
            admin.deleteTable(tablename);
            logger.info("Table: {} delete success!", tablename);
        } finally {
            closeConnect(conn);
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

    /**
     * 获取到rowkey的集合
     *
     * @param tableName
     * @return
     */
    public static List<String> getList(String tableName) {
        Table table = getTable(tableName);
        ResultScanner results;
        ArrayList<String> list = new ArrayList<String>();
        if (table != null) {
            try {
                Scan scan = new Scan();
                scan.setCaching(1000);
                results = table.getScanner(scan);
                for (Result result : results) {
                    List<Cell> cells = result.listCells();
                    for (Cell cell : cells) {
                        String row = Bytes.toString(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
                        list.add(row);
                    }
                }
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

        return list;
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
            get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
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
            if (gets != null && gets.size() > 0) {
                results = table.get(gets);
            }
        } catch (IOException e) {
            logger.error("getRows failure !", e);
        } finally {
            try {
                Objects.requireNonNull(table).close();
            } catch (IOException e) {
                logger.error("table.close() failure !", e);
            }
        }
        return results;
    }

    /**
     * 异步的往表里面添加数据
     *
     * @param rowKey
     * @param tablename
     * @param columnFamiliName
     * @param column
     * @param value
     * @throws Exception
     */
    //String rowKey, String tableName, String columnFamiliName, String[] column,String[] value
    public static void put(String rowKey, String tablename, String columnFamiliName, String[] column, String[] value)
            throws Exception {
        long currentTime = System.currentTimeMillis();
        Table htable = getConnection().getTable(TableName.valueOf(tablename));
        Put put = new Put(Bytes.toBytes(rowKey));
        HColumnDescriptor[] columnFamilies = htable.getTableDescriptor().getColumnFamilies();
        for (HColumnDescriptor columnFamily : columnFamilies) {
            String familyName = columnFamily.getNameAsString(); // 获取列族名
            if (familyName.equals(columnFamiliName)) {
                for (int j = 0; j < column.length; j++) {
                    put.addColumn(Bytes.toBytes(columnFamiliName), Bytes.toBytes(column[j]), Bytes.toBytes(value[j]));
                }
            }
        }
        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    logger.error("Failed to sent put " + e.getRow(i) + ".");
                }
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tablename)).listener(listener);
        params.writeBufferSize(5 * 1024 * 1024);
        try (BufferedMutator mutator = conn.getBufferedMutator(params)) {
            final List<Put> puts = Collections.singletonList(put);
            mutator.mutate(puts);
            mutator.flush();
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
        return put(tablename, (Put) Collections.singletonList(put));
    }

    /**
     * 往指定表添加数据
     *
     * @param tablename 表名
     * @param puts      需要添加的数据
     * @throws IOException
     */
    public static void putByHTable(String tablename, List<?> puts) throws Exception {
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
        System.currentTimeMillis();
    }

    //获取媒质信息
    public static List<MediaInfo> getRecommendList(String table) {
        List<MediaInfo> mediaInfoArrayList = new ArrayList<>();
        ResultScanner results = HBaseUtil.get(table);
        final PropertyUtil prop = PropertyUtil.getInstance();
        results.forEach(result -> {
            try {
                MediaInfo mediaInfo = new MediaInfo();
                String videoId = Bytes.toString(result.getRow());
                mediaInfo.setVideoId(videoId);
                for (Cell cell : result.rawCells()) {
                    if (prop.getString("videoName", "videoName").equalsIgnoreCase(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                        String videoname = Bytes.toString(CellUtil.cloneValue(cell));
                        mediaInfo.setVideoName(judgeValue(videoname));
                    }
                    if (prop.getString("directName", "directName").equalsIgnoreCase(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                        String directName = Bytes.toString(CellUtil.cloneValue(cell));
                        mediaInfo.setDriectorList(CommonUtil.getFilmerList(judgeValue(directName)));
                    }
                    if (prop.getString("actorName", "actorName").equalsIgnoreCase(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                        String actorName = Bytes.toString(CellUtil.cloneValue(cell));
                        mediaInfo.setActorList(CommonUtil.getFilmerList(actorName));
                    }
                    if (prop.getString("videoPlot", "videoPlot").equalsIgnoreCase(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                        String videoPlot = Bytes.toString(CellUtil.cloneValue(cell));
                        mediaInfo.setPlotList(CommonUtil.getFilmerList(judgeValue(videoPlot)));
                    }
                    if (prop.getString("videoRegion", "videoRegion").equalsIgnoreCase(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                        String videoRegion = Bytes.toString(CellUtil.cloneValue(cell));
                        mediaInfo.setRegions(CommonUtil.getCleanRegion(judgeValue(videoRegion)));
                    }
                    if (prop.getString("videoScore", "videoScore").equalsIgnoreCase(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                        String videoScore = Bytes.toString(CellUtil.cloneValue(cell));
                        if (StringUtils.isEmpty(videoScore)) {
                            videoScore = "0";
                        }
                        mediaInfo.setVideoScore(videoScore);
                    }
                    if (prop.getString("videoType", "videoType").equalsIgnoreCase(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                        String videoType = Bytes.toString(CellUtil.cloneValue(cell));
                        mediaInfo.setVideoType(judgeValue(videoType));
                    }
                }
                mediaInfoArrayList.add(mediaInfo);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        return mediaInfoArrayList;
    }

    private static String judgeValue(String value) {
        if (StringUtils.isEmpty(value)) {
            return "其他";
        }
        return value;
    }

}