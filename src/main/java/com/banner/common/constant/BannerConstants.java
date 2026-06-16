package com.banner.common.constant;

public class BannerConstants {

    // === 锁配置 ===
    public static final int LOCK_TIMEOUT_SECONDS = 30;
    public static final int MAX_RETRY_ATTEMPTS = 5;
    public static final long INITIAL_RETRY_INTERVAL_MS = 100;

    // === Kafka Topic ===
    public static final String KAFKA_TOPIC_BANNER_SYNC = "banner-sync-topic";
    public static final String KAFKA_CONSUMER_GROUP_ID = "banner-consumer-group";

    // === 日期格式 ===
    public static final String DATE_FORMAT = "yyyyMMdd";

    // === 分桶配置 ===
    public static final int USER_BUCKET_SIZE = 5000;
    public static final int USER_BUCKET_COUNT = 1000;  // 固定桶数量，用于取模定位

}
