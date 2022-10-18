package com.knowyou.util;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @author k
 */

public enum CateName {
    //电影分类
    FLIM("电影", "01"),
    TV("电视剧", "02"),
    CHILD("少儿", "03"),
    VARIETY("综艺", "04"),
    MUSIC("音乐", "05"),
    SPORT("体育", "06"),
    EDU("教育", "07"),
    RECORD("纪录片", "08"),
    ENTERTAINMENT("娱乐", "09"),
    FASHION("时尚", "10"),
    NEWS("新闻", "13"),
    LIFE("生活", "14"),
    COMIC("动漫", "15"),
    ESPORT("电竞", "16"),
    TRAILER("片花", "17"),
    FUNNY("搞笑", "18"),
    GAME("游戏", "19"),
    OTHER("其他", "20"),
    PARTYBUILDING("党建", "21"),
    SHOPPING("购物", "22"),
    PARENTCHILD("亲子", "23"),
    DOCUMENTARY("纪实", "24"),
    ALL("all", "98");


    private String cateName;
    private String cateNameCode;

    CateName(String cateName, String cateNameCode) {
        this.cateName = cateName;
        this.cateNameCode = cateNameCode;
    }

    public static List<HashMap<String, String>> toList() {
        ArrayList<HashMap<String, String>> list = new ArrayList<>();
        for (CateName cateName : CateName.values()) {
            HashMap<String, String> map = new HashMap<>();
            map.put("cateName", cateName.getCateName());
            map.put("cateNameCode", cateName.getCateNameCode());
            list.add(map);
        }
        return list;
    }


    public String getCateName() {
        return cateName;
    }

    public void setCateName(String cateName) {
        this.cateName = cateName;
    }

    public String getCateNameCode() {
        return cateNameCode;
    }

    public void setCateNameCode(String cateNameCode) {
        this.cateNameCode = cateNameCode;
    }
}
