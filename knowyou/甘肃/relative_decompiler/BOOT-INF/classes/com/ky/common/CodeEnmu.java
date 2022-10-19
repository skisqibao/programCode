package com.ky.common;

public enum CodeEnmu {
   COMMA(1, ","),
   SEMICOLON(2, ";"),
   COLON(3, ":"),
   DOLLAR(4, "$"),
   BLANK(5, " "),
   AXIS(6, "|");

   private int code;
   private String desc;

   private CodeEnmu(int code, String desc) {
      this.code = code;
      this.desc = desc;
   }

   public int getCode() {
      return this.code;
   }

   public String getDesc() {
      return this.desc;
   }
}
