package com.ky.common;

import java.io.Serializable;

public class ResponseResult implements Serializable {
   private int code;
   private String msg;
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
      return this.code;
   }

   public void setCode(int code) {
      this.code = code;
   }

   public String getMsg() {
      return this.msg;
   }

   public void setMsg(String msg) {
      this.msg = msg;
   }

   public Object getData() {
      return this.data;
   }

   public void setData(Object data) {
      this.data = data;
   }
}
