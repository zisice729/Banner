package com.banner.infrastructure.cache;

import com.banner.common.constant.BannerConstants;
import com.banner.common.util.DateUtil;
import com.banner.common.util.JsonUtil;
import com.banner.common.util.RedisKeyBuilder;
import com.banner.infrastructure.dto.BannerSyncRequest;
import com.github.benmanes.caffeine.cache.Cache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

@Component
public class BannerCacheManagerImpl implements BannerCacheManager {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private Cache<String, BannerSyncRequest> bannerLocalCache;

    @Autowired
    private Cache<String, List<Long>> bannerUserLocalCache;

    @Override
    public void refreshBannerCache(Long id, BannerSyncRequest data) {
        String bannerKey = RedisKeyBuilder.bannerCache(id);
        String productDateKey = RedisKeyBuilder.bannerProductDate(data.getProductId(), getCurrentDate(data));

        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            connection.set(bannerKey.getBytes(), JsonUtil.toJson(data).getBytes());
            connection.sAdd(productDateKey.getBytes(), String.valueOf(id).getBytes());
            connection.publish(RedisKeyBuilder.CACHE_INVALIDATE_CHANNEL.getBytes(), String.valueOf(id).getBytes());
            return null;
        });

        bannerLocalCache.invalidate(bannerKey);
    }

    @Override
    public void deleteBannerCache(Long id) {
        String bannerKey = RedisKeyBuilder.bannerCache(id);

        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            connection.del(bannerKey.getBytes());
            for (int i = 0; i < BannerConstants.USER_BUCKET_COUNT; i++) {
                connection.del(RedisKeyBuilder.bannerUserBucket(id, i).getBytes());
            }
            connection.publish(RedisKeyBuilder.CACHE_INVALIDATE_CHANNEL.getBytes(), String.valueOf(id).getBytes());
            return null;
        });

        bannerLocalCache.invalidate(bannerKey);
        invalidateUserLocalCache(id);
    }

    @Override
    public void clearUserBuckets(Long id) {
        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (int i = 0; i < BannerConstants.USER_BUCKET_COUNT; i++) {
                connection.del(RedisKeyBuilder.bannerUserBucket(id, i).getBytes());
            }
            return null;
        });

        invalidateUserLocalCache(id);
    }

    @Override
    public void writeUserBuckets(Long id, List<Long> userIds) {
        Map<Integer, List<byte[]>> bucketMap = new HashMap<>();

        for (Long userId : userIds) {
            int bucketIndex = RedisKeyBuilder.getUserBucketIndex(userId);
            bucketMap.computeIfAbsent(bucketIndex, k -> new ArrayList<>())
                    .add(String.valueOf(userId).getBytes());
        }

        redisTemplate.executePipelined((RedisCallback<Object>) connection -> {
            for (Map.Entry<Integer, List<byte[]>> entry : bucketMap.entrySet()) {
                String bucketKey = RedisKeyBuilder.bannerUserBucket(id, entry.getKey());
                List<byte[]> members = entry.getValue();
                byte[][] memberArray = members.toArray(new byte[0][]);
                connection.sAdd(bucketKey.getBytes(), memberArray);
            }
            return null;
        });
    }

    @Override
    public BannerSyncRequest getBannerFromCache(Long id) {
        String bannerKey = RedisKeyBuilder.bannerCache(id);

        BannerSyncRequest cached = bannerLocalCache.getIfPresent(bannerKey);
        if (Objects.nonNull(cached)) {
            return cached;
        }

        Object value = redisTemplate.opsForValue().get(bannerKey);
        if (Objects.isNull(value)) {
            return null;
        }

        BannerSyncRequest data = JsonUtil.fromJson(value.toString(), BannerSyncRequest.class);
        bannerLocalCache.put(bannerKey, data);
        return data;
    }

    @Override
    public List<Long> getUserIdsFromCache(Long id) {
        List<Long> allUserIds = new ArrayList<>();

        for (int i = 0; i < BannerConstants.USER_BUCKET_COUNT; i++) {
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
        int bucketIndex = RedisKeyBuilder.getUserBucketIndex(userId);
        String bucketKey = RedisKeyBuilder.bannerUserBucket(id, bucketIndex);
        Boolean isMember = redisTemplate.opsForSet().isMember(bucketKey, String.valueOf(userId));
        return Boolean.TRUE.equals(isMember);
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
        return DateUtil.formatYYYYMMDD(data.getStartTime());
    }

    private void invalidateUserLocalCache(Long id) {
        for (int i = 0; i < BannerConstants.USER_BUCKET_COUNT; i++) {
            bannerUserLocalCache.invalidate(RedisKeyBuilder.bannerUserBucket(id, i));
        }
    }
}
