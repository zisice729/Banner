package com.banner.common.constant;

public class BannerConstants {

    // === 锁配置 ===
    public static final int LOCK_TIMEOUT_SECONDS = 30;
    public static final int MAX_RETRY_ATTEMPTS = 5;
    public static final long INITIAL_RETRY_INTERVAL_MS = 100;

    // === 分桶配置 ===
    public static final int BUCKET_SIZE = 1000;

    // === 批次同步配置 ===
    public static final int BATCH_EXPIRE_MINUTES = 30;

    // === 定时任务 ===
    public static final int INCREMENTAL_WINDOW_MINUTES = 10;

    // === Kafka Topic ===
    public static final String KAFKA_TOPIC_BANNER = "banner-topic";
    public static final String KAFKA_TOPIC_BANNER_DELETE = "banner-delete-topic";
    public static final String KAFKA_TOPIC_USER_LIST_BATCH = "banner-userlist-batch-topic";
    public static final String KAFKA_TOPIC_USER_LIST_DELETE = "banner-userlist-delete-topic";
    public static final String KAFKA_CONSUMER_GROUP_ID = "banner-consumer-group";

    // === 日期格式 ===
    public static final String DATE_FORMAT = "yyyyMMdd";

}
