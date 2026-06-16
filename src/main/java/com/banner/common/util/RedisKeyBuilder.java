package com.banner.common.util;

public class RedisKeyBuilder {

    private static final String BANNER_CACHE_KEY = "banner:%d:info";
    private static final String BANNER_USER_BUCKET_KEY = "banner:%d:users:%d";
    private static final String BANNER_LOCK_KEY = "banner:%d:lock";
    private static final String BANNER_PRODUCT_DATE_KEY = "banner:product:%d:date:%s";
    private static final String BANNER_NO_USER_LIST_KEY = "banner:no_user_list";  // 无人群限制的Banner集合

    public static final String CACHE_INVALIDATE_CHANNEL = "banner:cache:invalidate";

    private RedisKeyBuilder() {
    }

    public static String bannerCache(Long id) {
        return String.format(BANNER_CACHE_KEY, id);
    }

    public static String bannerUserBucket(Long id, int bucketIndex) {
        return String.format(BANNER_USER_BUCKET_KEY, id, bucketIndex);
    }

    public static String bannerLock(Long id) {
        return String.format(BANNER_LOCK_KEY, id);
    }

    public static String bannerProductDate(Integer productId, String date) {
        return String.format(BANNER_PRODUCT_DATE_KEY, productId, date);
    }

    public static String bannerNoUserList() {
        return BANNER_NO_USER_LIST_KEY;
    }
}
