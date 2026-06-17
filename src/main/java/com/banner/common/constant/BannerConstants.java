package com.banner.common.constant;

/**
 * Banner系统常量定义
 * <p>
 * 统一管理Banner系统的常量配置，包括Kafka、分布式锁、幂等、分桶等配置。
 * </p>
 */
public class BannerConstants {

    /**
     * Kafka Topic名称
     * <p>
     * 用于Banner数据变更的同步消息，所有新增、更新、删除操作都发送到此Topic。
     * </p>
     */
    public static final String KAFKA_TOPIC_BANNER_SYNC = "banner-sync-topic";

    /**
     * Kafka消费者组ID
     * <p>
     * 同一组内的消费者会共同消费消息，每条消息只会被组内一个消费者处理。
     * </p>
     */
    public static final String KAFKA_CONSUMER_GROUP_ID = "banner-consumer-group";

    /**
     * 分布式锁超时时间（秒）
     * <p>
     * 锁的默认过期时间，防止锁未释放导致死锁。
     * 看门狗会每10秒自动续期，业务执行时间可以超过30秒。
     * </p>
     */
    public static final int LOCK_TIMEOUT_SECONDS = 30;

    /**
     * 锁获取最大重试次数
     * <p>
     * 获取锁失败时的最大重试次数，超过此次数后放弃获取。
     * </p>
     */
    public static final int MAX_RETRY_ATTEMPTS = 5;

    /**
     * 锁重试初始间隔（毫秒）
     * <p>
     * 获取锁失败时的初始重试间隔，后续按指数退避（100ms → 300ms → 900ms）。
     * </p>
     */
    public static final int INITIAL_RETRY_INTERVAL_MS = 100;

    /**
     * 幂等标记过期时间（天）
     * <p>
     * 幂等标记的过期时间，7天后自动删除，避免Redis Key堆积。
     * </p>
     */
    public static final int IDEMPOTENT_EXPIRE_DAYS = 7;

    /**
     * 用户桶数量
     * <p>
     * userId哈希分桶的固定桶数量，使用userId % 100计算桶索引。
     * 固定桶数量确保同一个用户始终分配到同一个桶，查询时O(1)定位。
     * </p>
     */
    public static final int USER_BUCKET_COUNT = 100;

    /**
     * RPC分页大小
     * <p>
     * 分页RPC获取用户列表时的每页大小，用于流式处理降低内存压力。
     * </p>
     */
    public static final int USER_BUCKET_SIZE = 1000;

    /**
     * RPC最大页数限制
     * <p>
     * 防止RPC调用无限循环，超过此页数后抛出异常。
     * 10000页 × 1000条/页 = 1000万用户，满足大部分场景。
     * </p>
     */
    public static final int MAX_RPC_PAGE_COUNT = 10000;

    private BannerConstants() {
    }
}
