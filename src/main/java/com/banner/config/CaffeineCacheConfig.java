package com.banner.config;

import com.banner.common.dto.request.BannerSyncRequest;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.TimeUnit;

@Configuration
public class CaffeineCacheConfig {

    @Bean
    public Cache<String, BannerSyncRequest> bannerLocalCache() {
        return Caffeine.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .build();
    }
}
