package com.banner.mq.producer;

import com.banner.common.constant.BannerConstants;
import com.banner.common.dto.mq.BannerMessage;
import com.banner.common.enums.MessageType;
import com.banner.common.util.JsonUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
public class BannerMqProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendBannerSync(Long id) {
        BannerMessage message = new BannerMessage();
        message.setMessageId(UUID.randomUUID().toString());
        message.setId(id);
        message.setType(MessageType.SYNC.getCode());
        kafkaTemplate.send(BannerConstants.KAFKA_TOPIC_BANNER_SYNC, JsonUtil.toJson(message));
    }

    public void sendBannerDelete(Long id) {
        BannerMessage message = new BannerMessage();
        message.setMessageId(UUID.randomUUID().toString());
        message.setId(id);
        message.setType(MessageType.DELETE.getCode());
        kafkaTemplate.send(BannerConstants.KAFKA_TOPIC_BANNER_SYNC, JsonUtil.toJson(message));
    }
}
