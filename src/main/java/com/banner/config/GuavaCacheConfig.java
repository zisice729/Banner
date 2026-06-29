package com.banner.config;

import com.banner.common.dto.Banner;
import com.banner.common.util.JsonUtil;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.core.RedisTemplate;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Guava本地缓存配置
 * 使用LoadingCache + CacheLoader实现两级缓存：本地缓存 → Redis缓存
 * 本地缓存无数据时，CacheLoader自动从Redis加载
 */
@Configuration
public class GuavaCacheConfig {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    /**
     * Banner详情本地缓存
     * Key: bannerInfo:bannerId:{bannerId}
     * Value: Banner对象
     * TTL: 5分钟
     * 最大容量：10000条
     */
    @Bean
    public LoadingCache<String, Banner> bannerLocalCache() {
        return CacheBuilder.newBuilder()
                .maximumSize(10000)
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .build(new CacheLoader<String, Banner>() {
                    /**
                     * 缓存加载器：本地缓存未命中时自动调用此方法从Redis加载
                     *
                     * @param bannerKey Redis Key（格式：bannerInfo:bannerId:{bannerId}）
                     * @return Banner对象，Redis中不存在返回null
                     */
                    @Override
                    public Banner load(String bannerKey) {
                        Object value = redisTemplate.opsForValue().get(bannerKey);
                        if (Objects.isNull(value)) {
                            return null;
                        }
                        return JsonUtil.fromJson(value.toString(), Banner.class);
                    }
                });
    }

    /**
     * 产品+日期索引本地缓存
     * Key: banners:productId:{productId}:date:{date}
     * Value: bannerId集合（Set<Object>）
     * TTL: 5分钟
     * 最大容量：1000条
     */
    @Bean
    public LoadingCache<String, Set<Object>> productDateLocalCache() {
        return CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(5, TimeUnit.MINUTES)
                .build(new CacheLoader<String, Set<Object>>() {
                    /**
                     * 缓存加载器：本地缓存未命中时自动调用此方法从Redis加载
                     *
                     * @param productDateKey Redis Key（格式：banners:productId:{productId}:date:{date}）
                     * @return bannerId集合，Redis中不存在返回null
                     */
                    @Override
                    public Set<Object> load(String productDateKey) {
                        Set<Object> bannerIds = redisTemplate.opsForSet().members(productDateKey);
                        return Objects.isNull(bannerIds) ? null : bannerIds;
                    }
                });
    }
}