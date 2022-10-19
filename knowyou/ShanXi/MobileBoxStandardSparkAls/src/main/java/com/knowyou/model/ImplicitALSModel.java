package com.knowyou.model;

import java.io.Serializable;


public class ImplicitALSModel implements Serializable {
    private String id;//主键id

    private byte[] model;// 模型

    private String createTime; //创建时间

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public byte[] getModel() {
        return model;
    }

    public void setModel(byte[] model) {
        this.model = model;
    }

    public String getCreateTime() {
        return createTime;
    }

    public void setCreateTime(String createTime) {
        this.createTime = createTime;
    }
}
