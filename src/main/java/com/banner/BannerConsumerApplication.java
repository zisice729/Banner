package com.banner;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class BannerConsumerApplication {

    public static void main(String[] args) {
        SpringApplication.run(BannerConsumerApplication.class, args);
    }

}
