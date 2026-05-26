package com.banner.service.impl;

import com.banner.common.constant.BannerConstants;
import com.banner.common.dto.BannerInfo;
import com.banner.common.util.JsonUtil;
import com.banner.service.BannerCacheManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Banner缓存管理实现类
 */
@Service
public class BannerCacheManagerImpl implements BannerCacheManager {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Override
    public void setBanner(String productId, String date, BannerInfo bannerInfo) {
        String cacheKey = BannerConstants.BANNER_CACHE_KEY_PREFIX + productId + ":" + date;

        String existingJson = (String) redisTemplate.opsForHash().get(cacheKey, bannerInfo.getBannerId());
        if (existingJson != null) {
            BannerInfo existing = JsonUtil.fromJson(existingJson, BannerInfo.class);
            if (existing.getUpdateTime() != null && existing.getUpdateTime() > bannerInfo.getUpdateTime()) {
                return;
            }
        }

        redisTemplate.opsForHash().put(cacheKey, bannerInfo.getBannerId(), JsonUtil.toJson(bannerInfo));
    }

    @Override
    public void deleteBanner(String productId, String date, String bannerId) {
        String cacheKey = BannerConstants.BANNER_CACHE_KEY_PREFIX + productId + ":" + date;
        redisTemplate.opsForHash().delete(cacheKey, bannerId);
    }

    @Override
    public List<BannerInfo> getBannersByDate(String productId, String date) {
        String cacheKey = BannerConstants.BANNER_CACHE_KEY_PREFIX + productId + ":" + date;

        Map<Object, Object> entries = redisTemplate.opsForHash().entries(cacheKey);
        if (entries == null || entries.isEmpty()) {
            return new ArrayList<>();
        }

        return entries.values().stream()
                .map(v -> JsonUtil.fromJson((String) v, BannerInfo.class))
                .filter(b -> b.getStatus() == 1)
                .sorted((a, b) -> b.getPriority().compareTo(a.getPriority()))
                .collect(Collectors.toList());
    }

    @Override
    public void refreshBannersByDate(String productId, String date, List<BannerInfo> banners) {
        String cacheKey = BannerConstants.BANNER_CACHE_KEY_PREFIX + productId + ":" + date;

        redisTemplate.delete(cacheKey);

        for (BannerInfo banner : banners) {
            if (banner.getStatus() == 1) {
                redisTemplate.opsForHash().put(cacheKey, banner.getBannerId(), JsonUtil.toJson(banner));
            }
        }
    }
}
