package com.banner.cache;

import com.banner.common.constant.BannerConstants;
import com.banner.common.util.RedisKeyBuilder;
import com.github.benmanes.caffeine.cache.Cache;
import com.banner.common.dto.request.BannerSyncRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.connection.Message;
import org.springframework.data.redis.connection.MessageListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
public class CacheInvalidationListener implements MessageListener {

    private static final Logger log = LoggerFactory.getLogger(CacheInvalidationListener.class);

    @Autowired
    private Cache<String, BannerSyncRequest> bannerLocalCache;

    @Override
    public void onMessage(Message message, byte[] pattern) {
        String body = new String(message.getBody(), StandardCharsets.UTF_8);
        try {
            Long id = Long.parseLong(body);
            String bannerKey = RedisKeyBuilder.bannerCache(id);

            // 清除本地缓存
            bannerLocalCache.invalidate(bannerKey);

            log.info("local cache invalidated by pub/sub, id={}", id);
        } catch (Exception e) {
            log.error("handle cache invalidation failed, body={}", body, e);
        }
    }
}
