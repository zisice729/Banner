package com.banner.common.constant;

public class BannerConstants {

    public static final String KAFKA_TOPIC_BANNER_SYNC = "banner-sync-topic";
    public static final String KAFKA_CONSUMER_GROUP_ID = "banner-consumer-group";

    public static final int LOCK_TIMEOUT_SECONDS = 30;

    public static final int IDEMPOTENT_EXPIRE_DAYS = 7;

    public static final int USER_BUCKET_SIZE = 1000;

    public static final int MAX_RPC_PAGE_COUNT = 10000;

    public static final int MAX_BUCKET_COUNT = 100;

    private BannerConstants() {
    }
}
