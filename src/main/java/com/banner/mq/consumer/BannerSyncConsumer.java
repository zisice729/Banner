package com.banner.mq.consumer;

import com.banner.common.constant.BannerConstants;
import com.banner.common.dto.BannerMessage;
import com.banner.common.enums.MessageType;
import com.banner.common.util.JsonUtil;
import com.banner.lock.RedisDistributedLock;
import com.banner.service.BannerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Kafka消息消费者
 * 消费Banner数据变更消息，同步到Redis缓存
 * 处理流程：幂等检查 → 判断操作类型 → 获取分布式锁 → 执行业务逻辑
 */
@Component
public class BannerSyncConsumer {

    @Autowired
    private BannerService bannerService;

    @Autowired
    private RedisDistributedLock redisDistributedLock;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    /**
     * 消费Banner同步消息
     * topic: banner-sync-topic，groupId: banner-consumer-group
     */
    @KafkaListener(topics = BannerConstants.KAFKA_TOPIC_BANNER_SYNC, groupId = BannerConstants.KAFKA_CONSUMER_GROUP_ID)
    public void consume(String message) {
        // 1. 解析消息
        BannerMessage bannerMessage = JsonUtil.fromJson(message, BannerMessage.class);
        Long bannerId = bannerMessage.getId();

        // 2. 幂等检查（Redis SET NX，7天过期）
        String idempotentKey = String.format("idempotent:messageId:%s", bannerMessage.getMessageId());
        Boolean isNew = redisTemplate.opsForValue()
                .setIfAbsent(idempotentKey, "1", BannerConstants.IDEMPOTENT_EXPIRE_DAYS, TimeUnit.DAYS);
        if (!Boolean.TRUE.equals(isNew)) {
            return;
        }

        // 3. DELETE类型：软删除，设置status=0，不删除人群包
        if (Objects.equals(bannerMessage.getType(), MessageType.DELETE.getCode())) {
            try {
                bannerService.setBannerInactive(bannerId);
            } catch (Exception e) {
                redisTemplate.delete(idempotentKey);
                throw e;
            }
            return;
        }

        // 4. 获取分布式锁（不重试，与定时任务共用同一锁key）
        // 锁key: bannerLock:bannerId:{bannerId}
        String lockKey = String.format("bannerLock:bannerId:%d", bannerId);
        String lockValue = redisDistributedLock.tryLock(lockKey);
        if (Objects.isNull(lockValue)) {
            redisTemplate.delete(idempotentKey);
            return;
        }

        // 5. 执行业务逻辑（同步Banner数据到Redis）
        try {
            bannerService.syncBanner(bannerId);
        } catch (Exception e) {
            redisTemplate.delete(idempotentKey);
            throw e;
        } finally {
            // 6. 释放锁
            redisDistributedLock.unlock(lockKey, lockValue);
        }
    }
}
