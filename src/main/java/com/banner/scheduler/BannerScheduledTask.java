package com.banner.scheduler;

import com.banner.client.BannerOperationClient;
import com.banner.common.dto.request.BannerSyncRequest;
import com.banner.service.BannerCacheManager;
import com.xxl.job.core.handler.annotation.XxlJob;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;

@Component
public class BannerScheduledTask {

    private static final Logger log = LoggerFactory.getLogger(BannerScheduledTask.class);

    @Autowired
    private BannerOperationClient bannerOperationClient;

    @Autowired
    private BannerCacheManager bannerCacheManager;

    @XxlJob("incrementalSyncJob")
    public void incrementalSync() {
        log.info("incrementalSync start");
        try {
            long sinceTime = System.currentTimeMillis() - 5 * 60 * 1000;
            List<Long> updatedIds = bannerOperationClient.getIncrementalUpdatedIds(sinceTime);

            for (Long id : updatedIds) {
                try {
                    BannerSyncRequest data = bannerOperationClient.getBannerById(id);
                    if (Objects.nonNull(data)) {
                        bannerCacheManager.refreshBannerCache(id, data);
                    }
                } catch (Exception e) {
                    log.error("incrementalSync single failed, id={}", id, e);
                }
            }
            log.info("incrementalSync completed, count={}", updatedIds.size());
        } catch (Exception e) {
            log.error("incrementalSync failed", e);
        }
    }

    @XxlJob("fullConsistencyCheckJob")
    public void fullConsistencyCheck() {
        log.info("fullConsistencyCheck start");
        try {
            List<Long> allIds = bannerOperationClient.getAllActiveBannerIds();

            for (Long id : allIds) {
                try {
                    BannerSyncRequest data = bannerOperationClient.getBannerById(id);
                    if (Objects.nonNull(data)) {
                        bannerCacheManager.refreshBannerCache(id, data);
                    } else {
                        // 运营端已无此数据，清除缓存
                        bannerCacheManager.deleteBannerCache(id);
                    }
                } catch (Exception e) {
                    log.error("fullConsistencyCheck single failed, id={}", id, e);
                }
            }
            log.info("fullConsistencyCheck completed, count={}", allIds.size());
        } catch (Exception e) {
            log.error("fullConsistencyCheck failed", e);
        }
    }
}
