package com.knowyou.util;

import java.io.Serializable;

public class Record implements Serializable {
    private int key;
    private String name;
    private int age;
    private String year;

    public Record(int key, String name, int age, String year) {
        this.key = key;
        this.name = name;
        this.age = age;
        this.year = year;
    }

    public int getKey() {
        return key;
    }

    public void setKey(int key) {
        this.key = key;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getAge() {
        return age;
    }

    public void setAge(int age) {
        this.age = age;
    }

    public String getYear() {
        return year;
    }

    public void setYear(String year) {
        this.year = year;
    }
}
