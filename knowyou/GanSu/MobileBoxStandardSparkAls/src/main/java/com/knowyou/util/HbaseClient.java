package com.knowyou.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * Created by wengxw on 2019/11/26
 * Hbase客户端工具类
 */
public class HbaseClient {

    public final static Logger log = LoggerFactory.getLogger(HbaseClient.class);

    private static Admin admin;
    private static Connection conn;

    static {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", Property.getStrValue("hbase.rootdir"));
        conf.set("hbase.zookeeper.quorum", Property.getStrValue("hbase.zookeeper.quorum"));
        conf.set("hbase.zookeeper.property.clientport", Property.getStrValue("hbase.zookeeper.property.clientport"));
        conf.set("hbase.client.scanner.timeout.period", Property.getStrValue("hbase.client.scanner.timeout.period"));
        conf.set("hbase.rpc.timeout", Property.getStrValue("hbase.rpc.timeout"));
        conf.set("zookeeper.znode.parent", Property.getStrValue("zookeeper.znode.parent"));
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createTable(String tableName, String... columnFamilies) throws IOException {
        TableName tablename = TableName.valueOf(tableName);
        if (admin.tableExists(tablename)) {
            log.error("Hbase Table Exists");
        } else {
            log.info("Start create table");
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
            for (String columnFamily : columnFamilies) {
                HTableDescriptor column = tableDescriptor.addFamily(new HColumnDescriptor(columnFamily));
            }
            admin.createTable(tableDescriptor);
            log.info("create table success");
        }
    }

    /**
     * 获取一列获取一行数据
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param column
     * @return
     * @throws IOException
     */
    public static String getData(String tableName, String rowKey, String familyName, String column) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        byte[] row = Bytes.toBytes(rowKey);
        Get get = new Get(row);
        Result result = table.get(get);
        byte[] resultValue = result.getValue(familyName.getBytes(), column.getBytes());
        if (null == resultValue) {
            return null;
        }
        return new String(resultValue);
    }

    /**
     * 获取一行所有数据 并且排序
     *
     * @param tableName
     * @param rowKey
     * @return
     * @throws IOException
     */
    public static List<Map.Entry> getRow(String tableName, String rowKey) throws IOException {
        Table table = conn.getTable(TableName.valueOf(tableName));
        byte[] row = Bytes.toBytes(rowKey);
        Get get = new Get(row);
        Result result = table.get(get);

        HashMap<String, Double> rst = new HashMap<>();

        for (Cell cell : result.listCells()) {
            String key = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength());
            String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
            rst.put(key, new Double(value));
        }

        List<Map.Entry> ans = new ArrayList<>();
        ans.addAll(rst.entrySet());

        Collections.sort(ans, (m1, m2) -> new Double((Double) m1.getValue() - (Double) m2.getValue()).intValue());

        return ans;
    }


    /**
     * 向对应列添加数据
     *
     * @param tableName
     * @param rowKey
     * @param familyName
     * @param column
     * @param data
     * @throws Exception
     */
    public static void putData(String tableName, String rowKey, String familyName, String column, String data) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(familyName.getBytes(), column.getBytes(), data.getBytes());
//        Put put = new Put(Bytes.toBytes(rowKey));
//        put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(column), Bytes.toBytes(data));


        table.put(put);
    }


    /**
     * 将该单元格的值加1
     *
     * @param tableName
     * @param rowkey
     * @param familyName
     * @param column
     * @throws Exception
     */
    public static void increamColumn(String tableName, String rowkey, String familyName, String column) throws Exception {
        String val = getData(tableName, rowkey, familyName, column);
        int res = 1;
        if (val != null) {
            res = Integer.valueOf(val) + 1;
        }
        putData(tableName, rowkey, familyName, column, String.valueOf(res));
    }

    /**
     * 取出表中所有的key
     *
     * @param tableName
     * @return
     * @throws IOException
     */
    public static List<String> getAllKey(String tableName) throws IOException {
        List<String> keys = new ArrayList<>();
        Scan scan = new Scan();
        Table table = HbaseClient.conn.getTable(TableName.valueOf(tableName));
        ResultScanner scanner = table.getScanner(scan);
        for (Result r : scanner) {
            keys.add(new String(r.getRow()));
        }
        return keys;
    }

    /**
     * 取出表中某列的数值TOP N 的rowkey
     *
     * @param tableName
     * @param familyName
     * @param column
     * @return
     * @throws IOException
     */
    public static List<String> getTopValueKey(String tableName, String familyName, String column) throws IOException {
        List<String> allKey = getAllKey(tableName);
        HashMap<String, Double> rst = new HashMap<>();

        for (String key : allKey) {
            String value = getData(tableName, key, familyName, column);
            rst.put(key, new Double(value));
        }
        List<Map.Entry> ans = new ArrayList<>();
        ans.addAll(rst.entrySet());
        Collections.sort(ans, (m1, m2) -> new Double((Double) m1.getValue() - (Double) m2.getValue()).intValue());
        List<Map.Entry> entries = new ArrayList<>();
        if (ans.size() > 5000) {
            entries = ans.subList(0, 5000);
        } else {
            entries = ans.subList(0, ans.size() - 1);
        }
        List<String> res = new ArrayList<>();
        for (Map.Entry m : entries) {
            res.add(String.valueOf(m.getKey()));
        }

        return res;
    }

    public static void main(String[] args) throws Exception {
        System.out.println(1);
/*        String data = HbaseClient.getData("hx_portraittag", "004604FF0004302008C360D21CAF0921", "info",
                "tj_active_level");
        System.out.println(data);
        String data1 = HbaseClient.getData("hx_portraittag", "004604FF0004302008C360D21CAF0921", "info",
                "tj_switch_frequency");
        System.out.println(data1);
        String data2 = HbaseClient.getData("hx_portraittag", "004604FF0004302008C360D21CAF0921", "info",
                "tj_watch_frequency");
        System.out.println(data2);
        String data3 = HbaseClient.getData("hx_portraittag", "004604FF0004302008C360D21CAF0921", "info",
                "family_label");
        System.out.println(data3);*/

        SparkSession sparkSession = SparkSession.builder().config("spark.sql.shuffle.partitions", 100).config("spark" +
                ".hadoop.validateOutputSpecs", "false").appName("saveAsHadoopDataset").enableHiveSupport().getOrCreate();
//.master("local[*]")


        sparkSession.sparkContext().setLogLevel("DEBUG");

        Dataset<Row> frame = sparkSession.sql("select device_id as rowkey,video_id from knowyou_jituan_dmt" +
                ".ads_rec_userdetail_wtr limit 5");


        frame.show(false);
        frame.foreach(
                (ForeachFunction<Row>) row -> {
                    String rowkey = row.getString(0);
                    String value = row.getString(1);
                    if (rowkey.length() != 0 && !"".equals(rowkey)) {
                        System.out.println(rowkey + "\t" + value);
                    }
                }
        );
        frame.javaRDD().foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                String rowkey = row.getString(0);
                String value = row.getString(1);
                if (rowkey.length() != 0 && !"".equals(rowkey)) {
                    HbaseClient.putData("test", rowkey + ".java", "info", "video_id", value);
                }
            }
        });

        sparkSession.stop();
    }
}
