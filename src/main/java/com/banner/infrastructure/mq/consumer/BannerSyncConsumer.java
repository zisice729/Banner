package com.banner.infrastructure.mq.consumer;

import com.banner.client.BannerOperationClient;
import com.banner.common.constant.BannerConstants;
import com.banner.common.enums.MessageType;
import com.banner.common.util.JsonUtil;
import com.banner.common.util.RedisKeyBuilder;
import com.banner.infrastructure.cache.BannerCacheManager;
import com.banner.infrastructure.dto.BannerMessage;
import com.banner.infrastructure.dto.BannerSyncRequest;
import com.banner.infrastructure.lock.RedisDistributedLock;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Kafka消息消费者
 * <p>
 * 负责消费banner-sync-topic消息，同步Banner数据到Redis缓存。
 * 处理流程：
 * 1. 幂等检查（Redis SET NX）
 * 2. 加分布式锁（看门狗自动续期）
 * 3. RPC获取Banner基本信息
 * 4. 分页RPC获取用户列表（流式处理）
 * 5. userId哈希分桶写入Redis
 * 6. 更新Banner信息缓存和索引
 * 7. 发布本地缓存失效通知
 * 8. 异常时删除幂等标记，允许Kafka重试
 * </p>
 */
@Component
public class BannerSyncConsumer {

    @Autowired
    private BannerOperationClient bannerOperationClient;

    @Autowired
    private BannerCacheManager bannerCacheManager;

    @Autowired
    private RedisDistributedLock redisDistributedLock;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @KafkaListener(topics = BannerConstants.KAFKA_TOPIC_BANNER_SYNC, groupId = BannerConstants.KAFKA_CONSUMER_GROUP_ID)
    public void consume(String message) {
        BannerMessage bannerMessage = JsonUtil.fromJson(message, BannerMessage.class);
        Long id = bannerMessage.getId();

        String idempotentKey = RedisKeyBuilder.idempotentKey(bannerMessage.getMessageId());
        Boolean isNew = redisTemplate.opsForValue()
                .setIfAbsent(idempotentKey, "1", BannerConstants.IDEMPOTENT_EXPIRE_DAYS, TimeUnit.DAYS);

        if (!Boolean.TRUE.equals(isNew)) {
            return;
        }

        if (Objects.equals(bannerMessage.getType(), MessageType.DELETE.getCode())) {
            try {
                bannerCacheManager.deleteBannerCache(id);
            } catch (Exception e) {
                redisTemplate.delete(idempotentKey);
                throw e;
            }
            return;
        }

        String lockKey = RedisKeyBuilder.bannerLock(id);
        String lockValue = redisDistributedLock.tryLockWithRetry(lockKey);
        if (Objects.isNull(lockValue)) {
            redisTemplate.delete(idempotentKey);
            throw new RuntimeException("Failed to acquire lock for banner sync: " + id);
        }

        try {
            BannerSyncRequest data = bannerOperationClient.getBannerById(id);
            if (Objects.isNull(data)) {
                redisTemplate.delete(idempotentKey);
                throw new RuntimeException("RPC getBannerById return null, id=" + id);
            }

            bannerCacheManager.clearUserBuckets(id);

            int bucketSize = BannerConstants.USER_BUCKET_SIZE;
            int page = 0;
            int maxPage = BannerConstants.MAX_RPC_PAGE_COUNT;

            while (page < maxPage) {
                List<Long> pageUserIds = bannerOperationClient.getUserIdsByPage(id, page, bucketSize);
                if (Objects.isNull(pageUserIds) || pageUserIds.isEmpty()) {
                    break;
                }
                bannerCacheManager.writeUserBuckets(id, pageUserIds);
                page++;
            }

            if (page >= maxPage) {
                redisTemplate.delete(idempotentKey);
                throw new RuntimeException("Exceed max page limit, id=" + id);
            }

            bannerCacheManager.refreshBannerCache(id, data);

        } catch (Exception e) {
            redisTemplate.delete(idempotentKey);
            throw e;
        } finally {
            redisDistributedLock.unlock(lockKey, lockValue);
        }
    }
}
