package com.banner.mq.consumer;

import com.banner.common.constant.BannerConstants;
import com.banner.common.dto.UserListBatchMessage;
import com.banner.common.util.JsonUtil;
import com.banner.service.BannerUserListService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class UserListConsumer {

    private static final Logger log = LoggerFactory.getLogger(UserListConsumer.class);

    @Autowired
    private BannerUserListService bannerUserListService;

    @KafkaListener(topics = BannerConstants.KAFKA_TOPIC_USER_LIST_BATCH, groupId = BannerConstants.KAFKA_CONSUMER_GROUP_ID)
    public void consumeUserListBatch(String message) {
        try {
            UserListBatchMessage batchMessage = JsonUtil.fromJson(message, UserListBatchMessage.class);
            bannerUserListService.syncUserListBatch(
                    batchMessage.getBannerId(),
                    batchMessage.getVersion(),
                    batchMessage.getBatchIndex(),
                    batchMessage.getTotalBatches(),
                    batchMessage.getTotalBuckets(),
                    batchMessage.getUserIds()
            );
        } catch (Exception e) {
            log.error("consumeUserListBatch failed, message={}", message, e);
        }
    }

    @KafkaListener(topics = BannerConstants.KAFKA_TOPIC_USER_LIST_DELETE, groupId = BannerConstants.KAFKA_CONSUMER_GROUP_ID)
    public void consumeUserListDelete(String message) {
        try {
            UserListBatchMessage deleteMessage = JsonUtil.fromJson(message, UserListBatchMessage.class);
            bannerUserListService.deleteUserList(deleteMessage.getBannerId());
        } catch (Exception e) {
            log.error("consumeUserListDelete failed, message={}", message, e);
        }
    }
}
