package com.knowyou.map;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.io.Serializable;

/**
 * row<deviceid,seriesheadcode,catalog> -> Tuple2<deviceid,tuple2<seriesheadcode,catalog>>
 */
public class RowToDeviceTupleFunc implements PairFunction<Row, String, Tuple2>, Serializable {
    @Override
    public Tuple2<String, Tuple2> call(Row row) throws Exception {
//        Tuple2 tuple2 = new Tuple2(row.getString(1), row.getString(2));

        // 添加评分后修改格式为 Tuple2<deviceid,tuple2<seriesheadcode,catalog#rating>>
        Tuple2 tuple2 = new Tuple2(row.getString(1), row.getString(2) + "#" + row.getDouble(3));

        return new Tuple2<>(row.getString(0), tuple2);
    }
}
