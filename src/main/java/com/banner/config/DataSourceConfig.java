package com.banner.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import jakarta.annotation.PostConstruct;

@Configuration
public class DataSourceConfig {

    private static final Logger log = LoggerFactory.getLogger(DataSourceConfig.class);

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @PostConstruct
    public void init() {
        jdbcTemplate.execute(
                "CREATE TABLE IF NOT EXISTS mq_consume_record (" +
                "  message_id VARCHAR(64) NOT NULL PRIMARY KEY, " +
                "  banner_id BIGINT NOT NULL, " +
                "  create_time BIGINT NOT NULL" +
                ")"
        );
        log.info("mq_consume_record table initialized");
    }
}
