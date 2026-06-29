package com.banner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * Banner消费端系统启动类
 * 基于Spring Boot 2.7构建，提供Banner查询和数据同步服务
 */
@SpringBootApplication
public class BannerApplication {

    public static void main(String[] args) {
        SpringApplication.run(BannerApplication.class, args);
    }
}