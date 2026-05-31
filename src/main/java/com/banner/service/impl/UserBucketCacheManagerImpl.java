package com.banner.service.impl;

import com.banner.common.util.BucketCalculator;
import com.banner.common.util.JsonUtil;
import com.banner.common.util.RedisKeyBuilder;
import com.banner.service.UserBucketCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

@Service
public class UserBucketCacheManagerImpl implements UserBucketCacheManager {

    private static final Logger log = LoggerFactory.getLogger(UserBucketCacheManagerImpl.class);

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Override
    public void setBucket(String bannerId, int bucketIndex, List<Long> userIds) {
        if (userIds == null || userIds.isEmpty()) {
            return;
        }
        String cacheKey = RedisKeyBuilder.userBucket(bannerId, bucketIndex);
        redisTemplate.opsForSet().add(cacheKey, userIds.toArray());
    }

    @Override
    public void deleteAllBuckets(String bannerId) {
        String pattern = RedisKeyBuilder.userBucketPattern(bannerId);
        deleteKeysByPattern(pattern);
        redisTemplate.delete(RedisKeyBuilder.userBucketVersion(bannerId));
        redisTemplate.delete(RedisKeyBuilder.userBucketTotal(bannerId));
    }

    @Override
    public boolean hasBucket(String bannerId) {
        String cacheKey = RedisKeyBuilder.userBucket(bannerId, 0);
        return Boolean.TRUE.equals(redisTemplate.hasKey(cacheKey));
    }

    @Override
    public boolean isUserInBucket(String bannerId, int bucketIndex, Long userId) {
        String cacheKey = RedisKeyBuilder.userBucket(bannerId, bucketIndex);
        return Boolean.TRUE.equals(redisTemplate.opsForSet().isMember(cacheKey, userId));
    }

    @Override
    public Integer getTotalBuckets(String bannerId) {
        String key = RedisKeyBuilder.userBucketTotal(bannerId);
        Object value = redisTemplate.opsForValue().get(key);
        return value != null ? Integer.parseInt(value.toString()) : null;
    }

    @Override
    public void setTotalBuckets(String bannerId, int totalBuckets) {
        String key = RedisKeyBuilder.userBucketTotal(bannerId);
        redisTemplate.opsForValue().set(key, String.valueOf(totalBuckets));
    }

    @Override
    public Long getVersion(String bannerId) {
        String key = RedisKeyBuilder.userBucketVersion(bannerId);
        Object value = redisTemplate.opsForValue().get(key);
        return value != null ? Long.parseLong(value.toString()) : null;
    }

    @Override
    public void setVersion(String bannerId, Long version) {
        String key = RedisKeyBuilder.userBucketVersion(bannerId);
        redisTemplate.opsForValue().set(key, String.valueOf(version));
    }

    @Override
    public void atomicSwitch(String bannerId, int totalBuckets, Map<Integer, List<Long>> bucketData, Long version) {
        deleteAllBuckets(bannerId);

        for (Map.Entry<Integer, List<Long>> entry : bucketData.entrySet()) {
            setBucket(bannerId, entry.getKey(), entry.getValue());
        }

        setTotalBuckets(bannerId, totalBuckets);

        if (version != null) {
            setVersion(bannerId, version);
        }
    }

    @Override
    public void setTempBatchData(String bannerId, Long version, int batchIndex, List<Long> userIds) {
        String key = RedisKeyBuilder.tempBatchData(bannerId, version, batchIndex);
        if (userIds != null && !userIds.isEmpty()) {
            redisTemplate.opsForSet().add(key, userIds.toArray());
            redisTemplate.expire(key, 2, TimeUnit.HOURS);
        }
    }

    @Override
    public void updateTempBatchMeta(String bannerId, Long version, int batchIndex, int totalBatches) {
        String key = RedisKeyBuilder.tempBatchMeta(bannerId, version);
        TempBatchMeta meta = getTempBatchMeta(bannerId, version);
        if (meta == null) {
            meta = new TempBatchMeta(version, totalBatches, new ArrayList<>(), System.currentTimeMillis());
        }
        if (!meta.getReceivedBatchIndices().contains(batchIndex)) {
            meta.getReceivedBatchIndices().add(batchIndex);
        }
        redisTemplate.opsForValue().set(key, JsonUtil.toJson(meta), 2, TimeUnit.HOURS);
    }

    @Override
    public TempBatchMeta getTempBatchMeta(String bannerId, Long version) {
        String key = RedisKeyBuilder.tempBatchMeta(bannerId, version);
        Object value = redisTemplate.opsForValue().get(key);
        if (value == null) {
            return null;
        }
        return JsonUtil.fromJson(value.toString(), TempBatchMeta.class);
    }

    @Override
    public boolean isAllBatchesReceived(String bannerId, Long version, int totalBatches) {
        TempBatchMeta meta = getTempBatchMeta(bannerId, version);
        if (meta == null) {
            return false;
        }
        return meta.getReceivedBatchIndices().size() >= totalBatches;
    }

    @Override
    public void mergeAndSwitch(String bannerId, Long version, int totalBuckets) {
        TempBatchMeta meta = getTempBatchMeta(bannerId, version);
        if (meta == null) {
            log.warn("mergeAndSwitch failed, meta not found, bannerId={}, version={}", bannerId, version);
            return;
        }

        Map<Integer, List<Long>> bucketData = new HashMap<>();
        for (int batchIndex : meta.getReceivedBatchIndices()) {
            String dataKey = RedisKeyBuilder.tempBatchData(bannerId, version, batchIndex);
            Set<Object> userIds = redisTemplate.opsForSet().members(dataKey);
            if (userIds != null) {
                List<Long> batchUserIds = new ArrayList<>();
                for (Object obj : userIds) {
                    batchUserIds.add(Long.parseLong(obj.toString()));
                }
                Map<Integer, List<Long>> distributed = BucketCalculator.distributeToBuckets(batchUserIds, totalBuckets);
                for (Map.Entry<Integer, List<Long>> entry : distributed.entrySet()) {
                    bucketData.computeIfAbsent(entry.getKey(), k -> new ArrayList<>()).addAll(entry.getValue());
                }
            }
        }

        atomicSwitch(bannerId, totalBuckets, bucketData, version);

        cleanTempData(bannerId, version);

        log.info("mergeAndSwitch completed, bannerId={}, version={}, totalBuckets={}, bucketCount={}",
                bannerId, version, totalBuckets, bucketData.size());
    }

    @Override
    public void cleanTempData(String bannerId, Long version) {
        String pattern = RedisKeyBuilder.tempBatchPattern(bannerId);
        deleteKeysByPattern(pattern);
    }

    @Override
    public void cleanExpiredTempData(int expireMinutes) {
        // 由定时任务调用，清理所有过期的临时数据
        // 实现略，可根据实际需求补充
    }

    private void deleteKeysByPattern(String pattern) {
        Cursor<byte[]> cursor = null;
        try {
            cursor = redisTemplate.getConnectionFactory()
                    .getConnection()
                    .scan(ScanOptions.scanOptions().match(pattern).count(100).build());
            Set<String> keys = new HashSet<>();
            while (cursor.hasNext()) {
                keys.add(new String(cursor.next()));
            }
            if (!keys.isEmpty()) {
                redisTemplate.delete(keys);
            }
        } catch (Exception e) {
            log.error("deleteKeysByPattern failed, pattern={}", pattern, e);
        } finally {
            if (cursor != null) {
                try {
                    cursor.close();
                } catch (Exception e) {
                    log.warn("close cursor failed", e);
                }
            }
        }
    }
}
