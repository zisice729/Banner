package com.banner.lock;

import com.banner.common.constant.BannerConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Redis分布式锁
 * <p>
 * 用于多实例部署时防止并发冲突。
 * 实现特点：
 * 1. 使用UUID作为锁值，保证锁的唯一性
 * 2. 释放锁时通过Lua脚本校验锁归属，防止误删其他线程的锁
 * 3. 支持重试机制，指数退避策略
 * </p>
 */
@Component
public class RedisDistributedLock {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    /**
     * 释放锁的Lua脚本
     * <p>
     * 先校验锁归属（比较锁值），再删除锁，保证原子性。
     * 如果锁值不匹配，说明锁已被其他线程持有，不执行删除。
     * </p>
     */
    private static final DefaultRedisScript<Long> UNLOCK_SCRIPT = new DefaultRedisScript<>(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            Long.class
    );

    /**
     * 尝试获取分布式锁
     * <p>
     * 使用SETNX命令实现，如果key不存在则设置成功。
     * 锁值使用UUID，保证唯一性；锁超时时间30秒，防止死锁。
     * </p>
     */
    public String tryLock(String productId, String date) {
        String lockKey = BannerConstants.LOCK_KEY_PREFIX + productId + ":" + date;
        String lockValue = UUID.randomUUID().toString();
        Boolean success = redisTemplate.opsForValue().setIfAbsent(
                lockKey, lockValue, BannerConstants.LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS
        );
        return Boolean.TRUE.equals(success) ? lockValue : null;
    }

    /**
     * 尝试获取分布式锁（带重试）
     * <p>
     * 使用指数退避策略重试，最多重试3次。
     * 重试间隔：100ms、300ms、900ms。
     * </p>
     */
    public String tryLockWithRetry(String productId, String date) {
        for (int i = 0; i < BannerConstants.MAX_RETRY_ATTEMPTS; i++) {
            String lockValue = tryLock(productId, date);
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

    /**
     * 释放分布式锁
     * <p>
     * 使用Lua脚本保证原子性：
     * 1. 先校验锁归属（比较锁值）
     * 2. 如果锁值匹配，删除锁
     * 3. 如果锁值不匹配，不执行删除
     * </p>
     */
    public void unlock(String productId, String date) {
        String lockKey = BannerConstants.LOCK_KEY_PREFIX + productId + ":" + date;
        redisTemplate.execute(UNLOCK_SCRIPT, Collections.singletonList(lockKey), lockValue);
    }

}
