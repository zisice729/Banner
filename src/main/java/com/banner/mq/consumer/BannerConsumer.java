package com.banner.mq.consumer;

import com.banner.common.constant.BannerConstants;
import com.banner.common.dto.BannerInfo;
import com.banner.common.util.JsonUtil;
import com.banner.service.BannerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Kafka消费者 - Banner变更消息处理
 */
@Component
public class BannerConsumer {

    @Autowired
    private BannerService bannerService;

    @KafkaListener(topics = BannerConstants.KAFKA_TOPIC_BANNER, groupId = BannerConstants.KAFKA_CONSUMER_GROUP_ID)
    public void consumeBannerUpdate(String message) {
        try {
            BannerInfo bannerInfo = JsonUtil.fromJson(message, BannerInfo.class);
            bannerInfo.setChangeType("UPDATE");
            bannerService.syncBannerFromKafka(bannerInfo);
        } catch (Exception e) {
        }
    }

    @KafkaListener(topics = BannerConstants.KAFKA_TOPIC_BANNER_DELETE, groupId = BannerConstants.KAFKA_CONSUMER_GROUP_ID)
    public void consumeBannerDelete(String message) {
        try {
            BannerInfo bannerInfo = JsonUtil.fromJson(message, BannerInfo.class);
            bannerInfo.setChangeType("DELETE");
            bannerService.syncBannerFromKafka(bannerInfo);
        } catch (Exception e) {
        }
    }
}
