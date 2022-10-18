package com.knowyou.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author Knowyou
 */

public enum FreePayType {
    //免费、付费
    FREE("free", " and package_series ='99' "),
    PAY("pay", " and package_series !='99' "),
    ALL("all", " "),
    ;

    private String freePayType ;
    private String sql ;

    FreePayType(String freePayType, String sql) {
        this.freePayType = freePayType;
        this.sql = sql;
    }

    public static List<HashMap<String, String>> toList() {
        ArrayList<HashMap<String, String>> list = new ArrayList<>();
        for (FreePayType type : FreePayType.values()) {
            HashMap<String, String> map = new HashMap<>();
            map.put("freePayType", type.getFreePayType());
            map.put("sql", type.getSql());
            list.add(map);
        }
        return list;
    }

    public String getFreePayType() {
        return freePayType;
    }

    public void setFreePayType(String freePayType) {
        this.freePayType = freePayType;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
