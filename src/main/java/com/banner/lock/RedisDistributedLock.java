package com.banner.lock;

import com.banner.common.constant.BannerConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Component
public class RedisDistributedLock {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    private static final String UNLOCK_SCRIPT =
            "if redis.call('get', KEYS[1]) == ARGV[1] then " +
            "    return redis.call('del', KEYS[1]) " +
            "else " +
            "    return 0 " +
            "end";

    public String tryLock(String lockKey) {
        // 1. 生成锁值（UUID）
        String lockValue = UUID.randomUUID().toString();
        // 2. SET NX + 过期时间，原子性获取锁
        Boolean success = redisTemplate.opsForValue().setIfAbsent(
                lockKey, lockValue, BannerConstants.LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS
        );
        // 3. 获取成功返回锁值，失败返回null（不重试）
        return Boolean.TRUE.equals(success) ? lockValue : null;
    }

    public boolean unlock(String lockKey, String lockValue) {
        // 1. 使用Lua脚本原子性释放锁
        // 2. 只有锁的值匹配时才删除，防止误删其他线程的锁
        DefaultRedisScript<Long> script = new DefaultRedisScript<>(UNLOCK_SCRIPT, Long.class);
        Long result = redisTemplate.execute(script, Collections.singletonList(lockKey), lockValue);
        return result != null && result == 1L;
    }
}
