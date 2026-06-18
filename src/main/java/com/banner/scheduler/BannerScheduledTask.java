package com.banner.scheduler;

import com.banner.client.BannerOperationClient;
import com.banner.common.dto.Banner;
import com.banner.lock.RedisDistributedLock;
import com.banner.service.BannerService;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;

/**
 * 定时任务（基于XXL-Job）
 * 职责：保障Banner数据最终一致性
 * 1. 增量同步：每5分钟同步最近更新的Banner
 * 2. 全量一致性检查：每30分钟检查所有Banner数据一致性
 */
@Component
public class BannerScheduledTask {

    @Autowired
    private BannerService bannerService;

    @Autowired
    private BannerOperationClient bannerOperationClient;

    @Autowired
    private RedisDistributedLock redisDistributedLock;

    /**
     * 增量同步任务
     * 每5分钟执行一次，同步最近5分钟内更新的Banner
     */
    @XxlJob("incrementalSyncJob")
    public void incrementalSync() {
        // 1. 获取最近5分钟更新的Banner ID列表
        long sinceTime = System.currentTimeMillis() - 5 * 60 * 1000;
        List<Long> updatedIds = bannerOperationClient.getIncrementalUpdatedIds(sinceTime);

        // 2. 逐个同步（带分布式锁）
        for (Long bannerId : updatedIds) {
            syncBanner(bannerId);
        }
    }

    /**
     * 全量一致性检查任务
     * 每30分钟执行一次，检查所有Banner数据与上游是否一致
     */
    @XxlJob("fullConsistencyCheckJob")
    public void fullConsistencyCheck() {
        // 1. 获取所有活跃Banner ID
        List<Long> allIds = bannerOperationClient.getAllActiveBannerIds();

        // 2. 逐个检查一致性
        for (Long bannerId : allIds) {
            Banner data = bannerOperationClient.getBannerById(bannerId);
            if (Objects.nonNull(data)) {
                // 上游存在：同步Banner数据
                syncBanner(bannerId);
            } else {
                // 上游不存在：设置为非活跃状态（软删除）
                bannerService.setBannerInactive(bannerId);
            }
        }
    }

    /**
     * 同步Banner数据（带分布式锁）
     * 与Kafka消费者共用同一锁key，防止并发处理
     */
    private void syncBanner(Long bannerId) {
        // 1. 获取分布式锁（不重试，与消费者共用同一锁key）
        // 锁key: bannerLock:bannerId:{bannerId}
        String lockKey = String.format("bannerLock:bannerId:%d", bannerId);
        String lockValue = redisDistributedLock.tryLock(lockKey);
        if (Objects.isNull(lockValue)) {
            return;
        }

        // 2. 执行业务逻辑（同步Banner数据到Redis）
        try {
            bannerService.syncBanner(bannerId);
        } finally {
            // 3. 释放锁
            redisDistributedLock.unlock(lockKey, lockValue);
        }
    }
}
