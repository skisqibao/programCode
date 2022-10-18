package com.ky.common;

import java.io.Serializable;

/**
 * @author wxt
 * @date 2019\6\21 0021 10:36
 **/
public class ResponseResult implements Serializable {
    /**
     * 错误码
     */
    private int code;
    /**
     * 错误信息
     */
    private String msg;
    /**
     * 响应数据
     */
    private Object data;

    public ResponseResult() {
    }

    ResponseResult(int code, String msg, Object data) {
        this.code = code;
        this.msg = msg;
        this.data = data;
    }
    ResponseResult(int code, Object data) {
        this.code = code;
        this.data = data;
    }
    ResponseResult(int code, String msg) {
        this.code = code;
        this.msg = msg;
    }
    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public Object getData() {
        return data;
    }

    public void setData(Object data) {
        this.data = data;
    }
}
