package com.banner.mq.producer;

import com.banner.common.constant.BannerConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Kafka消息生产者
 */
@Component
public class BannerMqProducer {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendBannerMessage(String message) {
        kafkaTemplate.send(BannerConstants.KAFKA_TOPIC_BANNER, message);
    }

}
