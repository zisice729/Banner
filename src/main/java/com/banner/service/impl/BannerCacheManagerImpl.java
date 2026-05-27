package com.banner.service.impl;

import com.banner.common.dto.SimpleBannerInfo;
import com.banner.common.util.JsonUtil;
import com.banner.common.util.RedisKeyBuilder;
import com.banner.service.BannerCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
public class BannerCacheManagerImpl implements BannerCacheManager {

    private static final Logger log = LoggerFactory.getLogger(BannerCacheManagerImpl.class);

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Override
    public void setBanner(String productId, String date, SimpleBannerInfo bannerInfo) {
        String cacheKey = RedisKeyBuilder.bannerCache(productId, date);

        String existingJson = (String) redisTemplate.opsForHash().get(cacheKey, bannerInfo.getBannerId());
        if (existingJson != null) {
            SimpleBannerInfo existing = JsonUtil.fromJson(existingJson, SimpleBannerInfo.class);
            if (existing.getVersion() != null && bannerInfo.getVersion() != null
                    && existing.getVersion() >= bannerInfo.getVersion()) {
                return;
            }
        }

        redisTemplate.opsForHash().put(cacheKey, bannerInfo.getBannerId(), JsonUtil.toJson(bannerInfo));
    }

    @Override
    public void deleteBanner(String productId, String date, String bannerId) {
        String cacheKey = RedisKeyBuilder.bannerCache(productId, date);
        redisTemplate.opsForHash().delete(cacheKey, bannerId);
    }

    @Override
    public List<SimpleBannerInfo> getBannersByDate(String productId, String date) {
        String cacheKey = RedisKeyBuilder.bannerCache(productId, date);

        Map<Object, Object> entries = redisTemplate.opsForHash().entries(cacheKey);
        if (entries == null || entries.isEmpty()) {
            return new ArrayList<>();
        }

        return entries.values().stream()
                .map(v -> JsonUtil.fromJson((String) v, SimpleBannerInfo.class))
                .filter(b -> b.getStatus() == 1)
                .sorted((a, b) -> b.getPriority().compareTo(a.getPriority()))
                .collect(Collectors.toList());
    }

    @Override
    public void refreshBannersByDate(String productId, String date, List<SimpleBannerInfo> banners) {
        String cacheKey = RedisKeyBuilder.bannerCache(productId, date);

        redisTemplate.delete(cacheKey);

        for (SimpleBannerInfo banner : banners) {
            if (banner.getStatus() == 1) {
                redisTemplate.opsForHash().put(cacheKey, banner.getBannerId(), JsonUtil.toJson(banner));
            }
        }
    }
}
