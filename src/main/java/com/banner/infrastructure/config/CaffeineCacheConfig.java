package com.banner.infrastructure.config;

import com.banner.infrastructure.dto.BannerSyncRequest;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Caffeine本地缓存配置
 * <p>
 * 配置两级缓存：
 * 1. bannerLocalCache - Banner基本信息缓存（10000条，5分钟过期）
 * 2. bannerUserLocalCache - 用户桶数据缓存（10000条，5分钟过期）
 * </p>
 */
@Configuration
public class CaffeineCacheConfig {

    @Bean
    public Cache<String, BannerSyncRequest> bannerLocalCache() {
        return Caffeine.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .build();
    }

    @Bean
    public Cache<String, List<Long>> bannerUserLocalCache() {
        return Caffeine.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .build();
    }
}
