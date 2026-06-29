package com.banner.common.constant;

/**
 * Banner系统常量定义
 * 包含：Kafka配置、分布式锁配置、幂等配置、分桶配置、RPC分页配置
 */
public class BannerConstants {

    /**
     * Kafka Topic名称：Banner数据同步Topic
     */
    public static final String KAFKA_TOPIC_BANNER_SYNC = "banner-sync-topic";

    /**
     * Kafka消费者组ID
     */
    public static final String KAFKA_CONSUMER_GROUP_ID = "banner-consumer-group";

    /**
     * 分布式锁超时时间（秒）
     * 当前未使用分布式锁，此常量保留备用
     */
    public static final int LOCK_TIMEOUT_SECONDS = 30;

    /**
     * 幂等Key过期时间（秒）
     * 使用Redis SET NX实现幂等，过期时间设为10秒
     */
    public static final int IDEMPOTENT_EXPIRE_SECONDS = 10;

    /**
     * 用户桶大小
     * 每个桶最多存放的用户数量，用于计算桶数量
     */
    public static final int USER_BUCKET_SIZE = 1000;

    /**
     * RPC最大分页数量
     * 防止分页RPC调用无限循环，超过此值抛异常
     */
    public static final int MAX_RPC_PAGE_COUNT = 10000;

    /**
     * 最大桶数量
     * 桶数量计算上限，避免桶数量过多
     */
    public static final int MAX_BUCKET_COUNT = 100;

    private BannerConstants() {
    }
}