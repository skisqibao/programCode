package com.ky;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@EnableTransactionManagement
@EnableScheduling
@EnableCaching
@SpringBootApplication
public class RelativeApplication {
   public static void main(String[] args) {
      SpringApplication.run(RelativeApplication.class, args);
   }
}
