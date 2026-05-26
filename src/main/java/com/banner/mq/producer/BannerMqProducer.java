package com.banner.mq.producer;

import com.banner.common.constant.BannerConstants;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

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

}
