package com.banner.mq.consumer;

import com.banner.common.constant.BannerConstants;
import com.banner.common.dto.BannerInfo;
import com.banner.common.util.JsonUtil;
import com.banner.repository.MessageProcessRecord;
import com.banner.repository.MessageProcessRecordRepository;
import com.banner.service.BannerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * Kafka消息消费者
 * <p>
 * 监听banner-topic主题，接收上游发送的Banner变更消息，
 * 经过幂等性校验后同步到Redis缓存。支持UPDATE和DELETE操作类型。
 * </p>
 */
@Component
public class BannerConsumer {

    @Autowired
    private BannerService bannerService;

    @Autowired
    private MessageProcessRecordRepository messageProcessRecordRepository;

    /**
     * 消息消费入口
     * <p>
     * 监听Kafka主题，接收Banner变更消息，流程：
     * 1. JSON反序列化为BannerInfo对象
     * 2. 幂等性校验（基于版本号）
     * 3. 调用业务层同步到Redis缓存
     * </p>
     */
    @KafkaListener(topics = BannerConstants.KAFKA_TOPIC_BANNER, groupId = BannerConstants.KAFKA_CONSUMER_GROUP_ID)
    @Transactional
    public void consumeBanner(String message) {
        BannerInfo bannerInfo = JsonUtil.fromJson(message, BannerInfo.class);

        if (!checkIdempotency(bannerInfo)) {
            return;
        }

        bannerService.syncBannerFromKafka(bannerInfo);
    }

    /**
     * 幂等性校验
     * <p>
     * 基于版本号机制防止重复消费：
     * 1. 查询该bannerId的最新处理记录
     * 2. 无历史记录 → 保存记录并通过
     * 3. 当前版本 ≤ 已处理版本 → 跳过（重复/过期）
     * 4. 当前版本 > 已处理版本 → 保存记录并通过
     * </p>
     */
    private boolean checkIdempotency(BannerInfo bannerInfo) {
        if (bannerInfo.getBannerId() == null) {
            return true;
        }

        MessageProcessRecord latest = messageProcessRecordRepository.findLatestByBannerId(bannerInfo.getBannerId());
        if (latest == null) {
            messageProcessRecordRepository.save(new MessageProcessRecord(bannerInfo.getBannerId(), bannerInfo.getVersion()));
            return true;
        }

        if (bannerInfo.getVersion() == null || bannerInfo.getVersion() <= latest.getVersion()) {
            return false;
        }

        messageProcessRecordRepository.save(new MessageProcessRecord(bannerInfo.getBannerId(), bannerInfo.getVersion()));
        return true;
    }
}
