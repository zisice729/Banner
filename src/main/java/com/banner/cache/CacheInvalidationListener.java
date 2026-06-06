package com.banner.cache;

import com.banner.common.util.RedisKeyBuilder;
import com.github.benmanes.caffeine.cache.Cache;
import com.banner.common.dto.request.BannerSyncRequest;

import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;

@Component
public class CacheInvalidationListener implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(CacheInvalidationListener.class);

    @Autowired
    private Cache<String, BannerSyncRequest> bannerLocalCache;

    @Autowired
    private Cache<String, List<Long>> bannerUserLocalCache;

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String body = new String(message.getBody(), StandardCharsets.UTF_8);
        try {
            Long id = Long.parseLong(body);
            String bannerKey = RedisKeyBuilder.bannerCache(id);
            String bucketCountKey = RedisKeyBuilder.bannerUserBucketCount(id);

            bannerLocalCache.invalidate(bannerKey);

            // 清除所有桶的本地缓存
            List<Long> bucketCountValue = bannerUserLocalCache.getIfPresent(bucketCountKey);
            if (bucketCountValue != null) {
                int bucketCount = bucketCountValue.size();
                for (int i = 0; i < bucketCount; i++) {
                    bannerUserLocalCache.invalidate(RedisKeyBuilder.bannerUserBucket(id, i));
                }
            }
            bannerUserLocalCache.invalidate(bucketCountKey);

            log.info("local cache invalidated by pub/sub, id={}", id);
        } catch (Exception e) {
            log.error("handle cache invalidation failed, body={}", body, e);
        }
    }
}
