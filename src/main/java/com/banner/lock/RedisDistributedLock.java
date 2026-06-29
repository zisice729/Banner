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
 * Redis分布式锁实现
 * 使用SET NX + Lua脚本实现，保证原子性和安全性
 * 注意：当前项目未使用分布式锁，此Bean保留备用
 */
@Component
public class RedisDistributedLock {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    /**
     * 解锁Lua脚本
     * 只有锁的值匹配时才删除，防止误删其他线程的锁
     */
    private static final String UNLOCK_SCRIPT =
            "if redis.call('get', KEYS[1]) == ARGV[1] then " +
            "    return redis.call('del', KEYS[1]) " +
            "else " +
            "    return 0 " +
            "end";

    /**
     * 尝试获取分布式锁
     * 使用SET NX + 过期时间，原子性获取锁
     * 获取失败返回null（不重试）
     *
     * @param lockKey 锁Key
     * @return 锁值（UUID），获取失败返回null
     */
    public String tryLock(String lockKey) {
        // Step1: 生成锁值（UUID），用于解锁时校验锁归属
        String lockValue = UUID.randomUUID().toString();
        // Step2: SET NX + 过期时间，原子性获取锁
        Boolean success = redisTemplate.opsForValue().setIfAbsent(
                lockKey, lockValue, BannerConstants.LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS
        );
        // Step3: 获取成功返回锁值，失败返回null（不重试）
        return Boolean.TRUE.equals(success) ? lockValue : null;
    }

    /**
     * 释放分布式锁
     * 使用Lua脚本原子性释放锁，防止误删其他线程的锁
     *
     * @param lockKey   锁Key
     * @param lockValue 锁值（获取锁时返回的UUID）
     * @return true-释放成功，false-释放失败（锁已过期或不属于当前线程）
     */
    public boolean unlock(String lockKey, String lockValue) {
        // Step1: 使用Lua脚本原子性释放锁
        // Step2: 只有锁的值匹配时才删除，防止误删其他线程的锁
        DefaultRedisScript<Long> script = new DefaultRedisScript<>(UNLOCK_SCRIPT, Long.class);
        Long result = redisTemplate.execute(script, Collections.singletonList(lockKey), lockValue);
        return result != null && result == 1L;
    }
}