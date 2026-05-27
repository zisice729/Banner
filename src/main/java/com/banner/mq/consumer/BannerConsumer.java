package com.banner.mq.consumer;

import com.banner.common.constant.BannerConstants;
import com.banner.common.dto.SimpleBannerInfo;
import com.banner.common.util.JsonUtil;
import com.banner.service.BannerSyncService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class BannerConsumer {

    private static final Logger log = LoggerFactory.getLogger(BannerConsumer.class);

    @Autowired
    private BannerSyncService bannerSyncService;

    @KafkaListener(topics = BannerConstants.KAFKA_TOPIC_BANNER, groupId = BannerConstants.KAFKA_CONSUMER_GROUP_ID)
    public void consumeBannerUpdate(String message) {
        try {
            SimpleBannerInfo bannerInfo = JsonUtil.fromJson(message, SimpleBannerInfo.class);
            bannerSyncService.syncBannerFromKafka(bannerInfo);
        } catch (Exception e) {
            log.error("consumeBannerUpdate failed, message={}", message, e);
        }
    }

    @KafkaListener(topics = BannerConstants.KAFKA_TOPIC_BANNER_DELETE, groupId = BannerConstants.KAFKA_CONSUMER_GROUP_ID)
    public void consumeBannerDelete(String message) {
        try {
            SimpleBannerInfo bannerInfo = JsonUtil.fromJson(message, SimpleBannerInfo.class);
            bannerSyncService.deleteBannerFromKafka(bannerInfo);
        } catch (Exception e) {
            log.error("consumeBannerDelete failed, message={}", message, e);
        }
    }
}
