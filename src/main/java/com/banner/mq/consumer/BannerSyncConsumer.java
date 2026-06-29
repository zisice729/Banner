package com.banner.mq.consumer;

import com.banner.common.constant.BannerConstants;
import com.banner.common.dto.BannerMessage;
import com.banner.common.enums.MessageType;
import com.banner.common.util.JsonUtil;
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
 * 处理流程：幂等检查 → 判断操作类型 → 执行业务逻辑
 * 注意：不使用分布式锁，因为消费者和定时任务都是从RPC获取最新数据，一致性有保证
 */
@Component
public class BannerSyncConsumer {

    @Autowired
    private BannerService bannerService;

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    /**
     * 消费Banner同步消息
     * topic: banner-sync-topic，groupId: banner-consumer-group
     *
     * @param message Kafka消息体（JSON格式）
     */
    @KafkaListener(topics = BannerConstants.KAFKA_TOPIC_BANNER_SYNC, groupId = BannerConstants.KAFKA_CONSUMER_GROUP_ID)
    public void consume(String message) {
        // Step1: 解析消息体
        BannerMessage bannerMessage = JsonUtil.fromJson(message, BannerMessage.class);
        Long bannerId = bannerMessage.getId();

        // Step2: 幂等检查（Redis SET NX，10秒过期）
        // 使用messageId作为幂等Key，重复消息直接跳过
        String idempotentKey = String.format("idempotent:messageId:%s", bannerMessage.getMessageId());
        Boolean isNew = redisTemplate.opsForValue()
                .setIfAbsent(idempotentKey, "1", BannerConstants.IDEMPOTENT_EXPIRE_SECONDS, TimeUnit.SECONDS);
        if (!Boolean.TRUE.equals(isNew)) {
            return;
        }

        // Step3: DELETE类型：软删除，设置status=0，不删除人群包
        if (Objects.equals(bannerMessage.getType(), MessageType.DELETE.getCode())) {
            bannerService.setBannerInactive(bannerId);
            return;
        }

        // Step4: UPDATE/CREATE类型：同步Banner数据到Redis
        bannerService.syncBanner(bannerId);
    }
}