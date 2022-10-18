package com.knowyou.map;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;

import java.io.Serializable;

/**
 * 获取spark.row中的第一列字符串
 */
public class RowFirstFunc implements Function<Row,String> , Serializable {

    @Override
    public String call(Row row) throws Exception {
        return row.getString(0);
    }
}
