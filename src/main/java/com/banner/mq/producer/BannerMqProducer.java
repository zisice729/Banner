package com.banner.mq.producer;

import com.banner.common.constant.BannerConstants;
import com.banner.common.dto.UserListBatchMessage;
import com.banner.common.util.JsonUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class BannerMqProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendBannerUpdate(String message) {
        kafkaTemplate.send(BannerConstants.KAFKA_TOPIC_BANNER, message);
    }

    public void sendBannerDelete(String message) {
        kafkaTemplate.send(BannerConstants.KAFKA_TOPIC_BANNER_DELETE, message);
    }

    public void sendUserListBatch(String message) {
        kafkaTemplate.send(BannerConstants.KAFKA_TOPIC_USER_LIST_BATCH, message);
    }

    public void sendUserListBatch(String bannerId, Long version, int totalBuckets, List<Long> userIds, int batchSize) {
        int totalUsers = userIds.size();
        int totalBatches = (int) Math.ceil((double) totalUsers / batchSize);

        for (int i = 0; i < totalBatches; i++) {
            int start = i * batchSize;
            int end = Math.min(start + batchSize, totalUsers);
            List<Long> batchUserIds = userIds.subList(start, end);

            UserListBatchMessage message = new UserListBatchMessage();
            message.setBannerId(bannerId);
            message.setVersion(version);
            message.setBatchIndex(i);
            message.setTotalBatches(totalBatches);
            message.setTotalBuckets(totalBuckets);
            message.setUserIds(batchUserIds);

            kafkaTemplate.send(BannerConstants.KAFKA_TOPIC_USER_LIST_BATCH, JsonUtil.toJson(message));
        }
    }

    public void sendUserListDelete(String bannerId) {
        UserListBatchMessage message = new UserListBatchMessage();
        message.setBannerId(bannerId);
        kafkaTemplate.send(BannerConstants.KAFKA_TOPIC_USER_LIST_DELETE, JsonUtil.toJson(message));
    }
}
