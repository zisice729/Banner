package com.banner.repository;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

@Repository
public class MqConsumeRecordRepository {

    private static final Logger log = LoggerFactory.getLogger(MqConsumeRecordRepository.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    public int insert(String messageId, Long id) {
        String sql = "INSERT INTO mq_consume_record (message_id, banner_id, create_time) VALUES (?, ?, ?)";
        return jdbcTemplate.update(sql, messageId, id, System.currentTimeMillis());
    }
}
