package com.banner.common.util;

import com.banner.common.constant.BannerConstants;

/**
 * Redis Key构建器
 * <p>
 * 统一管理Redis Key的命名规则，避免Key冲突。
 * Key设计：
 * 1. banner:{id}:info - Banner基本信息
 * 2. banner:{id}:users:{bucket} - 用户ID哈希分桶（固定100个桶）
 * 3. banner:{id}:lock - 分布式锁
 * 4. banner:product:{id}:date:{date} - 按产品+日期索引Banner ID
 * 5. idempotent:{messageId} - 幂等标记
 * </p>
 */
public class RedisKeyBuilder {

    private static final String BANNER_CACHE_KEY = "banner:%d:info";
    private static final String BANNER_USER_BUCKET_KEY = "banner:%d:users:%d";
    private static final String BANNER_LOCK_KEY = "banner:%d:lock";
    private static final String BANNER_PRODUCT_DATE_KEY = "banner:product:%d:date:%s";
    private static final String IDEMPOTENT_KEY = "idempotent:%s";

    public static final String CACHE_INVALIDATE_CHANNEL = "banner:cache:invalidate";

    private RedisKeyBuilder() {
    }

    /**
     * 构建幂等Key
     * <p>
     * 用于消息幂等消费，7天过期。
     * </p>
     *
     * @param messageId 消息ID
     * @return 幂等Key
     */
    public static String idempotentKey(String messageId) {
        return String.format(IDEMPOTENT_KEY, messageId);
    }

    /**
     * 构建Banner缓存Key
     * <p>
     * 用于存储Banner基本信息。
     * </p>
     *
     * @param id Banner ID
     * @return Banner缓存Key
     */
    public static String bannerCache(Long id) {
        return String.format(BANNER_CACHE_KEY, id);
    }

    /**
     * 构建用户桶Key
     * <p>
     * 用于存储用户ID哈希分桶数据，固定100个桶。
     * </p>
     *
     * @param id          Banner ID
     * @param bucketIndex 桶索引（0-99）
     * @return 用户桶Key
     */
    public static String bannerUserBucket(Long id, int bucketIndex) {
        return String.format(BANNER_USER_BUCKET_KEY, id, bucketIndex);
    }

    /**
     * 构建分布式锁Key
     * <p>
     * 用于防止并发更新缓存。
     * </p>
     *
     * @param id Banner ID
     * @return 分布式锁Key
     */
    public static String bannerLock(Long id) {
        return String.format(BANNER_LOCK_KEY, id);
    }

    /**
     * 构建产品日期索引Key
     * <p>
     * 用于按产品ID和日期索引Banner ID列表。
     * </p>
     *
     * @param productId 产品ID
     * @param date      日期（yyyyMMdd）
     * @return 产品日期索引Key
     */
    public static String bannerProductDate(Integer productId, String date) {
        return String.format(BANNER_PRODUCT_DATE_KEY, productId, date);
    }

    /**
     * 获取用户桶索引
     * <p>
     * 使用userId哈希固定分桶，确保同一个用户始终分配到同一个桶。
     * 桶数量固定为100，使用Math.abs防止负数取余。
     * </p>
     *
     * @param userId 用户ID
     * @return 桶索引（0-99）
     */
    public static int getUserBucketIndex(Long userId) {
        return (int) (Math.abs(userId) % BannerConstants.USER_BUCKET_COUNT);
    }
}
