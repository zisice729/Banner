package com.banner.scheduler;

import com.banner.common.entity.Banner;
import com.banner.common.util.DateUtil;
import com.banner.lock.RedisDistributedLock;
import com.banner.repository.BannerRepository;
import com.banner.service.BannerService;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 定时任务 - 数据一致性兜底
 * <p>
 * 通过XXL-Job定时任务保障Redis缓存与MySQL数据库的数据最终一致性：
 * 1. 增量刷新：处理最近10分钟更新的数据（在XXL-Job调度中心配置Cron：0 0/5 * * * ?）
 * 2. 全量一致性检查：检查所有有效Banner的数据一致性（在XXL-Job调度中心配置Cron：0 0/30 * * * ?）
 * </p>
 */
@Component
public class BannerScheduledTask {

    @Autowired
    private BannerRepository bannerRepository;

    @Autowired
    private BannerService bannerService;

    @Autowired
    private RedisDistributedLock redisDistributedLock;

    /**
     * 增量刷新任务
     * <p>
     * 查询最近10分钟更新的Banner数据，刷新到Redis缓存。
     * 获取分布式锁防止多实例并发刷新同一日期的数据。
     * 需要在XXL-Job调度中心配置Cron表达式：0 0/5 * * * ?
     * </p>
     */
    @XxlJob("bannerIncrementalRefreshJob")
    public void incrementalRefresh() {
        LocalDateTime tenMinutesAgo = DateUtil.minusMinutes(10);
        List<Banner> recentBanners = bannerRepository.findByUpdateTimeAfter(tenMinutesAgo);

        for (Banner banner : recentBanners) {
            List<String> dates = DateUtil.getDateRange(banner.getStartDay(), banner.getEndDay());

            for (String date : dates) {
                boolean locked = redisDistributedLock.tryLock(banner.getProductId(), date).isEmpty();
                if (!locked) {
                    continue;
                }

                try {
                    bannerService.refreshFromDatabase(banner.getProductId(), date);
                } finally {
                    redisDistributedLock.unlock(banner.getProductId(), date);
                }
            }
        }
    }

    /**
     * 全量一致性检查任务
     * <p>
     * 查询所有状态为启用的Banner数据，全量刷新到Redis缓存。
     * 获取分布式锁防止多实例并发刷新同一日期的数据。
     * 需要在XXL-Job调度中心配置Cron表达式：0 0/30 * * * ?
     * </p>
     */
    @XxlJob("bannerFullConsistencyCheckJob")
    public void fullConsistencyCheck() {
        List<Banner> allValidBanners = bannerRepository.findByStatus(1);

        for (Banner banner : allValidBanners) {
            List<String> dates = DateUtil.getDateRange(banner.getStartDay(), banner.getEndDay());

            for (String date : dates) {
                boolean locked = redisDistributedLock.tryLock(banner.getProductId(), date).isEmpty();
                if (!locked) {
                    continue;
                }

                try {
                    bannerService.refreshFromDatabase(banner.getProductId(), date);
                } finally {
                    redisDistributedLock.unlock(banner.getProductId(), date);
                }
            }
        }
    }
}
