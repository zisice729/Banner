package com.banner.service.impl;

import com.banner.common.constant.BannerConstants;
import com.banner.common.dto.request.BannerSyncRequest;
import com.banner.common.util.JsonUtil;
import com.banner.common.util.RedisKeyBuilder;
import com.github.benmanes.caffeine.cache.Cache;
import com.banner.service.BannerCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;

@Service
public class BannerCacheManagerImpl implements BannerCacheManager {

    private static final Logger log = LoggerFactory.getLogger(BannerCacheManagerImpl.class);

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private Cache<String, BannerSyncRequest> bannerLocalCache;

    @Autowired
    private Cache<String, List<Long>> bannerUserLocalCache;

    @Override
    public void refreshBannerCache(Long id, BannerSyncRequest data) {
        // 1. 更新Redis - Banner基本信息
        String bannerKey = RedisKeyBuilder.bannerCache(id);
        redisTemplate.opsForValue().set(bannerKey, JsonUtil.toJson(data));

        // 2. 更新Redis - 用户列表分桶存储
        // 先删除旧桶
        String bucketCountKey = RedisKeyBuilder.bannerUserBucketCount(id);
        Object oldCountObj = redisTemplate.opsForValue().get(bucketCountKey);
        if (Objects.nonNull(oldCountObj)) {
            int oldCount = Integer.parseInt(oldCountObj.toString());
            for (int i = 0; i < oldCount; i++) {
                redisTemplate.delete(RedisKeyBuilder.bannerUserBucket(id, i));
            }
        }

        // 写入新桶
        List<Long> userIds = data.getUserIds();
        if (Objects.nonNull(userIds) && !userIds.isEmpty()) {
            int bucketSize = BannerConstants.USER_BUCKET_SIZE;
            int bucketCount = (userIds.size() + bucketSize - 1) / bucketSize;

            for (int i = 0; i < bucketCount; i++) {
                int from = i * bucketSize;
                int to = Math.min(from + bucketSize, userIds.size());
                List<Long> bucketUserIds = userIds.subList(from, to);

                String bucketKey = RedisKeyBuilder.bannerUserBucket(id, i);
                String[] userIdStrs = bucketUserIds.stream()
                        .map(String::valueOf)
                        .toArray(String[]::new);
                redisTemplate.opsForSet().add(bucketKey, (Object[]) userIdStrs);
            }

            // 记录桶数量
            redisTemplate.opsForValue().set(bucketCountKey, String.valueOf(bucketCount));
        } else {
            redisTemplate.delete(bucketCountKey);
        }

        // 3. 更新按product+date索引
        if (Objects.nonNull(data.getProductId())) {
            String productDateKey = RedisKeyBuilder.bannerProductDate(data.getProductId(), getCurrentDate(data));
            redisTemplate.opsForSet().add(productDateKey, String.valueOf(id));
        }

        // 4. 发布本地缓存失效通知（所有机器包括本机都会收到）
        redisTemplate.convertAndSend(RedisKeyBuilder.CACHE_INVALIDATE_CHANNEL, String.valueOf(id));

        log.info("refreshBannerCache completed, id={}", id);
    }

    @Override
    public void deleteBannerCache(Long id) {
        // 1. 删除Redis - Banner信息
        String bannerKey = RedisKeyBuilder.bannerCache(id);
        redisTemplate.delete(bannerKey);

        // 2. 删除Redis - 所有用户桶
        String bucketCountKey = RedisKeyBuilder.bannerUserBucketCount(id);
        Object countObj = redisTemplate.opsForValue().get(bucketCountKey);
        if (Objects.nonNull(countObj)) {
            int bucketCount = Integer.parseInt(countObj.toString());
            for (int i = 0; i < bucketCount; i++) {
                redisTemplate.delete(RedisKeyBuilder.bannerUserBucket(id, i));
            }
        }
        redisTemplate.delete(bucketCountKey);

        // 3. 发布本地缓存失效通知
        redisTemplate.convertAndSend(RedisKeyBuilder.CACHE_INVALIDATE_CHANNEL, String.valueOf(id));

        log.info("deleteBannerCache completed, id={}", id);
    }

    @Override
    public BannerSyncRequest getBannerFromCache(Long id) {
        String bannerKey = RedisKeyBuilder.bannerCache(id);

        // 1. 先查本地缓存
        BannerSyncRequest cached = bannerLocalCache.getIfPresent(bannerKey);
        if (Objects.nonNull(cached)) {
            return cached;
        }

        // 2. 查Redis
        Object value = redisTemplate.opsForValue().get(bannerKey);
        if (Objects.isNull(value)) {
            return null;
        }

        BannerSyncRequest data = JsonUtil.fromJson(value.toString(), BannerSyncRequest.class);
        // 回填本地缓存
        bannerLocalCache.put(bannerKey, data);
        return data;
    }

    @Override
    public List<Long> getUserIdsFromCache(Long id) {
        // 查询所有桶
        String bucketCountKey = RedisKeyBuilder.bannerUserBucketCount(id);

        // 1. 先查本地缓存的桶数量
        List<Long> bucketCountCached = bannerUserLocalCache.getIfPresent(bucketCountKey);
        int bucketCount;
        if (Objects.nonNull(bucketCountCached) && !bucketCountCached.isEmpty()) {
            bucketCount = bucketCountCached.size();
        } else {
            Object countObj = redisTemplate.opsForValue().get(bucketCountKey);
            if (Objects.isNull(countObj)) {
                return new ArrayList<>();
            }
            bucketCount = Integer.parseInt(countObj.toString());
        }

        // 2. 遍历所有桶获取userId
        List<Long> allUserIds = new ArrayList<>();
        for (int i = 0; i < bucketCount; i++) {
            String bucketKey = RedisKeyBuilder.bannerUserBucket(id, i);

            List<Long> cachedBucket = bannerUserLocalCache.getIfPresent(bucketKey);
            if (Objects.nonNull(cachedBucket)) {
                allUserIds.addAll(cachedBucket);
                continue;
            }

            Set<Object> members = redisTemplate.opsForSet().members(bucketKey);
            if (Objects.nonNull(members) && !members.isEmpty()) {
                List<Long> bucketUserIds = members.stream()
                        .map(obj -> Long.parseLong(obj.toString()))
                        .toList();
                allUserIds.addAll(bucketUserIds);
                bannerUserLocalCache.put(bucketKey, bucketUserIds);
            }
        }

        return allUserIds;
    }

    @Override
    public boolean containsUserId(Long id, Long userId) {
        String bucketCountKey = RedisKeyBuilder.bannerUserBucketCount(id);
        Object countObj = redisTemplate.opsForValue().get(bucketCountKey);
        if (Objects.isNull(countObj)) {
            return true;
        }
        int bucketCount = Integer.parseInt(countObj.toString());

        // 在所有桶中查找
        for (int i = 0; i < bucketCount; i++) {
            String bucketKey = RedisKeyBuilder.bannerUserBucket(id, i);
            Boolean isMember = redisTemplate.opsForSet().isMember(bucketKey, String.valueOf(userId));
            if (Boolean.TRUE.equals(isMember)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public List<BannerSyncRequest> getBannersByProductAndDate(Integer productId, String date) {
        String productDateKey = RedisKeyBuilder.bannerProductDate(productId, date);
        Set<Object> bannerIds = redisTemplate.opsForSet().members(productDateKey);
        if (Objects.isNull(bannerIds) || bannerIds.isEmpty()) {
            return new ArrayList<>();
        }

        return bannerIds.stream()
                .map(obj -> {
                    Long id = Long.parseLong(obj.toString());
                    BannerSyncRequest banner = getBannerFromCache(id);
                    if (Objects.nonNull(banner) && Objects.equals(banner.getStatus(), 1)) {
                        return banner;
                    }
                    return null;
                })
                .filter(Objects::nonNull)
                .sorted((a, b) -> {
                    if (Objects.isNull(a.getPriority())) return 1;
                    if (Objects.isNull(b.getPriority())) return -1;
                    return b.getPriority().compareTo(a.getPriority());
                })
                .toList();
    }

    private String getCurrentDate(BannerSyncRequest data) {
        if (Objects.nonNull(data.getStartTime())) {
            return String.valueOf(data.getStartTime());
        }
        return com.banner.common.util.DateUtil.todayStr();
    }
}
