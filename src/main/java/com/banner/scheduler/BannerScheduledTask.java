package com.banner.scheduler;

import com.banner.common.constant.BannerConstants;
import com.banner.common.entity.SimpleBanner;
import com.banner.common.util.DateUtil;
import com.banner.common.util.RedisKeyBuilder;
import com.banner.lock.RedisDistributedLock;
import com.banner.repository.SimpleBannerRepository;
import com.banner.service.BannerSyncService;
import com.banner.service.BannerUserListService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Component
public class BannerScheduledTask {

    private static final Logger log = LoggerFactory.getLogger(BannerScheduledTask.class);

    @Autowired
    private SimpleBannerRepository simpleBannerRepository;

    @Autowired
    private BannerSyncService bannerSyncService;

    @Autowired
    private BannerUserListService bannerUserListService;

    @Autowired
    private RedisDistributedLock redisDistributedLock;

    @Scheduled(fixedRate = 5 * 60 * 1000)
    public void incrementalRefresh() {
        try {
            LocalDateTime updateTime = LocalDateTime.now().minusMinutes(BannerConstants.INCREMENTAL_WINDOW_MINUTES);
            List<SimpleBanner> banners = simpleBannerRepository.findByUpdateTimeAfter(updateTime);

            Map<String, List<SimpleBanner>> groupedByProduct = banners.stream()
                    .collect(Collectors.groupingBy(SimpleBanner::getProductId));

            for (Map.Entry<String, List<SimpleBanner>> entry : groupedByProduct.entrySet()) {
                String productId = entry.getKey();

                for (SimpleBanner banner : entry.getValue()) {
                    refreshBannerAndUserList(productId, banner);
                }
            }
        } catch (Exception e) {
            log.error("incrementalRefresh failed", e);
        }
    }

    @Scheduled(fixedRate = 30 * 60 * 1000)
    public void fullConsistencyCheck() {
        try {
            List<SimpleBanner> banners = simpleBannerRepository.findByStatus(1);

            Map<String, List<SimpleBanner>> groupedByProduct = banners.stream()
                    .collect(Collectors.groupingBy(SimpleBanner::getProductId));

            for (Map.Entry<String, List<SimpleBanner>> entry : groupedByProduct.entrySet()) {
                String productId = entry.getKey();

                for (SimpleBanner banner : entry.getValue()) {
                    refreshBannerAndUserList(productId, banner);
                }
            }
        } catch (Exception e) {
            log.error("fullConsistencyCheck failed", e);
        }
    }

    private void refreshBannerAndUserList(String productId, SimpleBanner banner) {
        List<String> dates = DateUtil.getDateRange(banner.getStartDay(), banner.getEndDay());

        for (String date : dates) {
            String lockKey = RedisKeyBuilder.bannerLock(productId, date);
            String lockValue = redisDistributedLock.tryLock(lockKey);
            if (lockValue == null) {
                log.warn("refreshBannerAndUserList failed to acquire lock, productId={}, date={}", productId, date);
                continue;
            }

            try {
                bannerSyncService.refreshFromDatabase(productId, date);
                bannerUserListService.refreshFromDatabase(banner.getBannerId());
            } finally {
                redisDistributedLock.unlock(lockKey, lockValue);
            }
        }
    }
}
