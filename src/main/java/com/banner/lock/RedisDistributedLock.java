package com.banner.lock;

import com.banner.common.constant.BannerConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class RedisDistributedLock {

    private static final Logger log = LoggerFactory.getLogger(RedisDistributedLock.class);

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    private static final String UNLOCK_SCRIPT =
            "if redis.call('get', KEYS[1]) == ARGV[1] then " +
            "    return redis.call('del', KEYS[1]) " +
            "else " +
            "    return 0 " +
            "end";

    public String tryLock(String lockKey) {
        String lockValue = UUID.randomUUID().toString();
        Boolean success = redisTemplate.opsForValue().setIfAbsent(
                lockKey, lockValue, BannerConstants.LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS
        );
        return Boolean.TRUE.equals(success) ? lockValue : null;
    }

    public String tryLockWithRetry(String lockKey) {
        for (int i = 0; i < BannerConstants.MAX_RETRY_ATTEMPTS; i++) {
            String lockValue = tryLock(lockKey);
            if (lockValue != null) {
                return lockValue;
            }
            try {
                long waitTime = (long) (BannerConstants.INITIAL_RETRY_INTERVAL_MS * Math.pow(3, i));
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return null;
            }
        }
        return null;
    }

    public boolean unlock(String lockKey, String lockValue) {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>(UNLOCK_SCRIPT, Long.class);
        Long result = redisTemplate.execute(script, Collections.singletonList(lockKey), lockValue);
        return result != null && result == 1L;
    }

    public Map<String, String> tryLockAll(List<String> lockKeys) {
        Map<String, String> locks = new LinkedHashMap<>();
        for (String lockKey : lockKeys) {
            String lockValue = tryLockWithRetry(lockKey);
            if (lockValue == null) {
                unlockAll(locks);
                return null;
            }
            locks.put(lockKey, lockValue);
        }
        return locks;
    }

    public void unlockAll(Map<String, String> locks) {
        if (locks == null || locks.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> entry : locks.entrySet()) {
            try {
                unlock(entry.getKey(), entry.getValue());
            } catch (Exception e) {
                log.warn("unlock failed, lockKey={}", entry.getKey(), e);
            }
        }
    }
}
