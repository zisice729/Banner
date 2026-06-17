package com.banner.infrastructure.cache;

import com.banner.common.constant.BannerConstants;
import com.banner.common.util.RedisKeyBuilder;
import com.github.benmanes.caffeine.cache.Cache;
import com.banner.infrastructure.dto.BannerSyncRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * 本地缓存失效监听器
 * <p>
 * 监听Redis Pub/Sub频道，接收缓存失效通知，清除本地缓存。
 * 保障多机环境下本地缓存的一致性。
 * </p>
 */
@Component
public class CacheInvalidationListener implements MessageListener {

    @Autowired
    private Cache<String, BannerSyncRequest> bannerLocalCache;

    @Autowired
    private Cache<String, List<Long>> bannerUserLocalCache;

    /**
     * 接收Redis Pub/Sub消息
     * <p>
     * 消息体为Banner ID，清除本地缓存中对应的Banner信息和用户桶数据。
     * </p>
     *
     * @param message Redis消息
     * @param pattern 订阅模式
     */
    @Override
    public void onMessage(Message message, byte[] pattern) {
        String body = new String(message.getBody(), StandardCharsets.UTF_8);
        try {
            Long id = Long.parseLong(body);
            String bannerKey = RedisKeyBuilder.bannerCache(id);

            bannerLocalCache.invalidate(bannerKey);

            for (int i = 0; i < BannerConstants.USER_BUCKET_COUNT; i++) {
                bannerUserLocalCache.invalidate(RedisKeyBuilder.bannerUserBucket(id, i));
            }
        } catch (Exception e) {
            // ignore invalid message
        }
    }
}
