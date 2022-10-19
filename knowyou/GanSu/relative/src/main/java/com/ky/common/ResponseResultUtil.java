package com.ky.common;

/**
 * @author wxt
 * @date 2019/9/5 18:09
 **/
public class ResponseResultUtil {

    public static ResponseResult getFailRequest(String erroMsg) {
        return new ResponseResult(1, erroMsg, null);
    }

    public static ResponseResult getSuccessRequest(Object data) {
        return new ResponseResult(0, data);
    }

    public static ResponseResult getSuccessRequest() {
        return new ResponseResult(0, null);
    }

}
