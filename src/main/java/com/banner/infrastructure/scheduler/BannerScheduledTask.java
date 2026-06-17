package com.banner.infrastructure.scheduler;

import com.banner.client.BannerOperationClient;
import com.banner.common.constant.BannerConstants;
import com.banner.infrastructure.cache.BannerCacheManager;
import com.banner.infrastructure.dto.BannerSyncRequest;
import com.banner.infrastructure.lock.RedisDistributedLock;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;

/**
 * Banner定时任务
 * <p>
 * 基于XXL-Job实现分布式定时任务，保障缓存与数据库的最终一致性。
 * 任务类型：
 * 1. 增量同步：每5分钟同步最近更新的Banner数据
 * 2. 全量一致性检查：每30分钟检查所有活跃Banner数据的一致性
 * </p>
 */
@Component
public class BannerScheduledTask {

    @Autowired
    private BannerOperationClient bannerOperationClient;

    @Autowired
    private BannerCacheManager bannerCacheManager;

    @Autowired
    private RedisDistributedLock redisDistributedLock;

    /**
     * 增量同步任务
     * <p>
     * 每5分钟执行一次，同步最近5分钟更新的Banner数据。
     * 使用分布式锁防止多机同时同步同一个Banner。
     * </p>
     */
    @XxlJob("incrementalSyncJob")
    public void incrementalSync() {
        long sinceTime = System.currentTimeMillis() - 5 * 60 * 1000;
        List<Long> updatedIds = bannerOperationClient.getIncrementalUpdatedIds(sinceTime);

        for (Long id : updatedIds) {
            syncBanner(id);
        }
    }

    /**
     * 全量一致性检查任务
     * <p>
     * 每30分钟执行一次，检查所有活跃Banner数据的一致性。
     * 如果RPC返回null则删除缓存，否则同步数据。
     * </p>
     */
    @XxlJob("fullConsistencyCheckJob")
    public void fullConsistencyCheck() {
        List<Long> allIds = bannerOperationClient.getAllActiveBannerIds();

        for (Long id : allIds) {
            BannerSyncRequest data = bannerOperationClient.getBannerById(id);
            if (Objects.nonNull(data)) {
                syncBanner(id);
            } else {
                bannerCacheManager.deleteBannerCache(id);
            }
        }
    }

    /**
     * 同步单个Banner数据
     * <p>
     * 使用分布式锁防止并发同步，流式处理用户列表降低内存压力。
     * </p>
     *
     * @param id Banner ID
     */
    private void syncBanner(Long id) {
        String lockKey = "banner:scheduled:" + id;
        String lockValue = redisDistributedLock.tryLock(lockKey);
        if (Objects.isNull(lockValue)) {
            return;
        }

        try {
            BannerSyncRequest data = bannerOperationClient.getBannerById(id);
            if (Objects.isNull(data)) {
                bannerCacheManager.deleteBannerCache(id);
                return;
            }

            bannerCacheManager.clearUserBuckets(id);

            int bucketSize = BannerConstants.USER_BUCKET_SIZE;
            int page = 0;
            int maxPage = BannerConstants.MAX_RPC_PAGE_COUNT;

            while (page < maxPage) {
                List<Long> pageUserIds = bannerOperationClient.getUserIdsByPage(id, page, bucketSize);
                if (Objects.isNull(pageUserIds) || pageUserIds.isEmpty()) {
                    break;
                }
                bannerCacheManager.writeUserBuckets(id, pageUserIds);
                page++;
            }

            bannerCacheManager.refreshBannerCache(id, data);
        } finally {
            redisDistributedLock.unlock(lockKey, lockValue);
        }
    }
}
