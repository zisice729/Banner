package com.banner.infrastructure.lock;

import com.banner.common.constant.BannerConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Component;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Redis分布式锁
 * <p>
 * 特性：
 * 1. 看门狗自动续期：获取锁后每10秒自动续期30秒，防止业务执行时间长导致锁过期
 * 2. 指数退避重试：获取锁失败时按指数退避重试（100ms → 300ms → 900ms）
 * 3. Lua脚本释放：使用Lua脚本原子性释放锁，防止误删其他线程的锁
 * </p>
 */
@Component
public class RedisDistributedLock {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    private final ScheduledExecutorService renewExecutor = Executors.newScheduledThreadPool(4);
    private final ConcurrentHashMap<String, ScheduledFuture<?>> renewTasks = new ConcurrentHashMap<>();

    private static final String UNLOCK_SCRIPT =
            "if redis.call('get', KEYS[1]) == ARGV[1] then " +
            "    return redis.call('del', KEYS[1]) " +
            "else " +
            "    return 0 " +
            "end";

    private static final long RENEW_INTERVAL_SECONDS = 10;

    /**
     * 尝试获取分布式锁
     * <p>
     * 使用SET NX命令原子性获取锁，成功后启动看门狗自动续期
     * </p>
     *
     * @param lockKey 锁的Key
     * @return 锁的值（UUID），获取失败返回null
     */
    public String tryLock(String lockKey) {
        String lockValue = UUID.randomUUID().toString();
        Boolean success = redisTemplate.opsForValue().setIfAbsent(
                lockKey, lockValue, BannerConstants.LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS
        );
        if (Boolean.TRUE.equals(success)) {
            startWatchdog(lockKey, lockValue);
        }
        return Boolean.TRUE.equals(success) ? lockValue : null;
    }

    /**
     * 尝试获取分布式锁（带重试）
     * <p>
     * 获取锁失败时按指数退避重试，最多重试5次
     * </p>
     *
     * @param lockKey 锁的Key
     * @return 锁的值（UUID），获取失败返回null
     */
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

    /**
     * 释放分布式锁
     * <p>
     * 使用Lua脚本原子性释放锁，只有锁的值匹配时才删除，防止误删其他线程的锁
     * 释放锁前停止看门狗
     * </p>
     *
     * @param lockKey   锁的Key
     * @param lockValue 锁的值
     * @return 是否释放成功
     */
    public boolean unlock(String lockKey, String lockValue) {
        stopWatchdog(lockKey, lockValue);
        DefaultRedisScript<Long> script = new DefaultRedisScript<>(UNLOCK_SCRIPT, Long.class);
        Long result = redisTemplate.execute(script, Collections.singletonList(lockKey), lockValue);
        return result != null && result == 1L;
    }

    /**
     * 批量获取多个分布式锁
     * <p>
     * 依次获取多个锁，如果其中一个获取失败，则释放已获取的所有锁
     * </p>
     *
     * @param lockKeys 锁的Key列表
     * @return 锁的Map（Key -> Value），获取失败返回null
     */
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

    /**
     * 批量释放多个分布式锁
     *
     * @param locks 锁的Map（Key -> Value）
     */
    public void unlockAll(Map<String, String> locks) {
        if (locks == null || locks.isEmpty()) {
            return;
        }
        for (Map.Entry<String, String> entry : locks.entrySet()) {
            unlock(entry.getKey(), entry.getValue());
        }
    }

    /**
     * 启动看门狗
     * <p>
     * 启动定时任务，每10秒检查锁是否存在，存在则续期30秒
     * </p>
     *
     * @param lockKey   锁的Key
     * @param lockValue 锁的值
     */
    private void startWatchdog(String lockKey, String lockValue) {
        String taskKey = lockKey + ":" + lockValue;
        ScheduledFuture<?> future = renewExecutor.scheduleAtFixedRate(
                () -> renewLock(lockKey, lockValue),
                RENEW_INTERVAL_SECONDS, RENEW_INTERVAL_SECONDS, TimeUnit.SECONDS
        );
        renewTasks.put(taskKey, future);
    }

    /**
     * 停止看门狗
     *
     * @param lockKey   锁的Key
     * @param lockValue 锁的值
     */
    private void stopWatchdog(String lockKey, String lockValue) {
        String taskKey = lockKey + ":" + lockValue;
        ScheduledFuture<?> future = renewTasks.remove(taskKey);
        if (future != null) {
            future.cancel(false);
        }
    }

    /**
     * 续期锁
     * <p>
     * 检查锁的值是否匹配，匹配则续期30秒
     * </p>
     *
     * @param lockKey   锁的Key
     * @param lockValue 锁的值
     */
    private void renewLock(String lockKey, String lockValue) {
        Object currentValue = redisTemplate.opsForValue().get(lockKey);
        if (lockValue.equals(currentValue)) {
            redisTemplate.expire(lockKey, BannerConstants.LOCK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }
    }
}
