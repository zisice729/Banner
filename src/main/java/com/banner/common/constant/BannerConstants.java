package com.banner.common.constant;

public class BannerConstants {

    public static final String BANNER_CACHE_KEY_PREFIX = "banner:";
    public static final String LOCK_KEY_PREFIX = "lock:banner:";

    public static final int LOCK_TIMEOUT_SECONDS = 30;
    public static final int MAX_RETRY_ATTEMPTS = 5;
    public static final long INITIAL_RETRY_INTERVAL_MS = 100;
    public static final int INCREMENTAL_WINDOW_MINUTES = 10;

    public static final String KAFKA_TOPIC_BANNER = "banner-topic";
    public static final String KAFKA_TOPIC_BANNER_DELETE = "banner-delete-topic";
    public static final String KAFKA_CONSUMER_GROUP_ID = "banner-consumer-group";

    public static final String DATE_FORMAT = "yyyyMMdd";

}
