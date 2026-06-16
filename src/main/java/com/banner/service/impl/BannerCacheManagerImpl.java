package com.banner.service.impl;

import com.banner.common.constant.BannerConstants;
import com.banner.common.dto.request.BannerSyncRequest;
import com.banner.common.util.DateUtil;
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

    @Override
    public void refreshBannerCache(Long id, BannerSyncRequest data) {
        // 1. 更新Redis - Banner基本信息
        String bannerKey = RedisKeyBuilder.bannerCache(id);
        redisTemplate.opsForValue().set(bannerKey, JsonUtil.toJson(data));

        // 2. 更新Redis - 用户列表（按取模分桶）
        List<Long> userIds = data.getUserIds();
        if (Objects.isNull(userIds) || userIds.isEmpty()) {
            // 无人群限制，添加到无用户列表集合
            redisTemplate.opsForSet().add(RedisKeyBuilder.bannerNoUserList(), String.valueOf(id));
        } else {
            // 有人群限制，从无用户列表集合移除
            redisTemplate.opsForSet().remove(RedisKeyBuilder.bannerNoUserList(), String.valueOf(id));

            // 按userId取模分配到固定桶
            int bucketCount = BannerConstants.USER_BUCKET_COUNT;
            for (Long userId : userIds) {
                int bucketIndex = (int) (userId % bucketCount);
                String bucketKey = RedisKeyBuilder.bannerUserBucket(id, bucketIndex);
                redisTemplate.opsForSet().add(bucketKey, String.valueOf(userId));
            }
        }

        // 3. 更新按product+date索引
        if (Objects.nonNull(data.getProductId())) {
            String productDateKey = RedisKeyBuilder.bannerProductDate(data.getProductId(), getCurrentDate(data));
            redisTemplate.opsForSet().add(productDateKey, String.valueOf(id));
        }

        // 4. 发布本地缓存失效通知
        redisTemplate.convertAndSend(RedisKeyBuilder.CACHE_INVALIDATE_CHANNEL, String.valueOf(id));

        log.info("refreshBannerCache completed, id={}, userCount={}", id, 
                Objects.isNull(userIds) ? 0 : userIds.size());
    }

    @Override
    public void deleteBannerCache(Long id) {
        // 1. 删除Redis - Banner信息
        String bannerKey = RedisKeyBuilder.bannerCache(id);
        redisTemplate.delete(bannerKey);

        // 2. 删除Redis - 所有用户桶（固定1000个桶）
        int bucketCount = BannerConstants.USER_BUCKET_COUNT;
        for (int i = 0; i < bucketCount; i++) {
            redisTemplate.delete(RedisKeyBuilder.bannerUserBucket(id, i));
        }

        // 3. 从无用户列表集合移除
        redisTemplate.opsForSet().remove(RedisKeyBuilder.bannerNoUserList(), String.valueOf(id));

        // 4. 发布本地缓存失效通知
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
        List<Long> allUserIds = new ArrayList<>();
        int bucketCount = BannerConstants.USER_BUCKET_COUNT;

        // 遍历所有桶获取userId
        for (int i = 0; i < bucketCount; i++) {
            String bucketKey = RedisKeyBuilder.bannerUserBucket(id, i);
            Set<Object> members = redisTemplate.opsForSet().members(bucketKey);
            if (Objects.nonNull(members) && !members.isEmpty()) {
                List<Long> bucketUserIds = members.stream()
                        .map(obj -> Long.parseLong(obj.toString()))
                        .toList();
                allUserIds.addAll(bucketUserIds);
            }
        }

        return allUserIds;
    }

    @Override
    public boolean containsUserId(Long id, Long userId) {
        // 1. 快速判断：是否在无人群限制集合中
        Boolean isNoUserList = redisTemplate.opsForSet()
                .isMember(RedisKeyBuilder.bannerNoUserList(), String.valueOf(id));
        if (Boolean.TRUE.equals(isNoUserList)) {
            return true;  // 无人群限制，对所有用户可见
        }

        // 2. 取模定位桶，O(1)时间复杂度
        int bucketIndex = (int) (userId % BannerConstants.USER_BUCKET_COUNT);
        String bucketKey = RedisKeyBuilder.bannerUserBucket(id, bucketIndex);
        Boolean isMember = redisTemplate.opsForSet().isMember(bucketKey, String.valueOf(userId));

        return Boolean.TRUE.equals(isMember);
    }

    @Override
    public List<BannerSyncRequest> getBannersByProductAndDate(Integer productId, String date) {
        if (Objects.isNull(date) || date.trim().isEmpty()) {
            date = DateUtil.todayStr();
        }

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
        return DateUtil.todayStr();
    }
}
