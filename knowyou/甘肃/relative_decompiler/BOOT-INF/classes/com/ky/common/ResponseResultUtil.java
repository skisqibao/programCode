package com.ky.common;

public class ResponseResultUtil {
   public static ResponseResult getFailRequest(String erroMsg) {
      return new ResponseResult(1, erroMsg, (Object)null);
   }

   public static ResponseResult getSuccessRequest(Object data) {
      return new ResponseResult(0, data);
   }

   public static ResponseResult getSuccessRequest() {
      return new ResponseResult(0, (String)null);
   }
}
