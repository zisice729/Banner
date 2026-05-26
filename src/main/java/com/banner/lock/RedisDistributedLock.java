package com.banner.lock;

import com.banner.common.constant.BannerConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class RedisDistributedLock {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    public boolean tryLock(String productId, String date) {
        String lockKey = BannerConstants.LOCK_KEY_PREFIX + productId + ":" + date;
        Boolean success = redisTemplate.opsForValue().setIfAbsent(
                lockKey, "1", BannerConstants.LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS
        );
        return Boolean.TRUE.equals(success);
    }

    public boolean tryLockWithRetry(String productId, String date) {
        for (int i = 0; i < BannerConstants.MAX_RETRY_ATTEMPTS; i++) {
            if (tryLock(productId, date)) {
                return true;
            }
            try {
                long waitTime = (long) (BannerConstants.INITIAL_RETRY_INTERVAL_MS * Math.pow(3, i));
                Thread.sleep(waitTime);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return false;
    }

    public void unlock(String productId, String date) {
        String lockKey = BannerConstants.LOCK_KEY_PREFIX + productId + ":" + date;
        redisTemplate.delete(lockKey);
    }

}
