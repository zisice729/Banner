package com.banner.scheduler;

import com.banner.common.entity.Banner;
import com.banner.common.util.DateUtil;
import com.banner.lock.RedisDistributedLock;
import com.banner.repository.BannerRepository;
import com.banner.service.BannerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

/**
 * Banner定时任务 - 兜底刷新
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
     * 增量刷新 - 每5分钟执行一次
     */
    @Scheduled(cron = "0 0/5 * * * ?")
    public void incrementalRefresh() {
        LocalDateTime tenMinutesAgo = DateUtil.minusMinutes(10);
        List<Banner> recentBanners = bannerRepository.findByUpdateTimeAfter(tenMinutesAgo);

        for (Banner banner : recentBanners) {
            List<String> dates = DateUtil.getDateRange(banner.getStartDay(), banner.getEndDay());

            for (String date : dates) {
                boolean locked = redisDistributedLock.tryLock(banner.getProductId(), date);
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
     * 全量一致性检查 - 每30分钟执行一次
     */
    @Scheduled(cron = "0 0/30 * * * ?")
    public void fullConsistencyCheck() {
        List<Banner> allValidBanners = bannerRepository.findByStatus(1);

        for (Banner banner : allValidBanners) {
            List<String> dates = DateUtil.getDateRange(banner.getStartDay(), banner.getEndDay());

            for (String date : dates) {
                boolean locked = redisDistributedLock.tryLock(banner.getProductId(), date);
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
