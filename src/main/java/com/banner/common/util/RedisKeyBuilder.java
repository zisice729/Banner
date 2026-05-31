package com.banner.common.util;

public class RedisKeyBuilder {

    private static final String BANNER_CACHE_PREFIX = "banner:";
    private static final String BANNER_LOCK_PREFIX = "lock:banner:";
    private static final String USER_BUCKET_PREFIX = "banner:user:bucket:";
    private static final String USER_BUCKET_VERSION_PREFIX = "banner:user:version:";
    private static final String USER_BUCKET_TOTAL_PREFIX = "banner:user:total:";
    private static final String USER_BUCKET_TEMP_PREFIX = "banner:user:temp:";

    private RedisKeyBuilder() {
    }

    public static String bannerCache(String productId, String date) {
        return BANNER_CACHE_PREFIX + productId + ":" + date;
    }

    public static String bannerLock(String productId, String date) {
        return BANNER_LOCK_PREFIX + productId + ":" + date;
    }

    public static String userBucket(String bannerId, int bucketIndex) {
        return USER_BUCKET_PREFIX + bannerId + ":" + bucketIndex;
    }

    public static String userBucketPattern(String bannerId) {
        return USER_BUCKET_PREFIX + bannerId + ":*";
    }

    public static String userBucketVersion(String bannerId) {
        return USER_BUCKET_VERSION_PREFIX + bannerId;
    }

    public static String userBucketTotal(String bannerId) {
        return USER_BUCKET_TOTAL_PREFIX + bannerId;
    }

    public static String tempBatchData(String bannerId, Long version, int batchIndex) {
        return USER_BUCKET_TEMP_PREFIX + bannerId + ":" + version + ":batch:" + batchIndex;
    }

    public static String tempBatchMeta(String bannerId, Long version) {
        return USER_BUCKET_TEMP_PREFIX + bannerId + ":" + version + ":meta";
    }

    public static String tempBatchPattern(String bannerId) {
        return USER_BUCKET_TEMP_PREFIX + bannerId + ":*";
    }
}
