package com.banner.common.util;

public class RedisKeyBuilder {

    private static final String BANNER_CACHE_KEY = "banner:%d:info";
    private static final String BANNER_USER_BUCKET_KEY = "banner:%d:users:%d";
    private static final String BANNER_USER_BUCKET_COUNT_KEY = "banner:%d:users:bucket_count";
    private static final String BANNER_LOCK_KEY = "banner:%d:lock";
    private static final String BANNER_PRODUCT_DATE_KEY = "banner:product:%d:date:%s";

    public static final String CACHE_INVALIDATE_CHANNEL = "banner:cache:invalidate";

    private RedisKeyBuilder() {
    }

    public static String bannerCache(Long id) {
        return String.format(BANNER_CACHE_KEY, id);
    }

    public static String bannerUserBucket(Long id, int bucketIndex) {
        return String.format(BANNER_USER_BUCKET_KEY, id, bucketIndex);
    }

    public static String bannerUserBucketCount(Long id) {
        return String.format(BANNER_USER_BUCKET_COUNT_KEY, id);
    }

    public static String bannerLock(Long id) {
        return String.format(BANNER_LOCK_KEY, id);
    }

    public static String bannerProductDate(Integer productId, String date) {
        return String.format(BANNER_PRODUCT_DATE_KEY, productId, date);
    }
}
