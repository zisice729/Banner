package com.banner.scheduler;

import com.banner.client.BannerOperationClient;
import com.banner.common.dto.Banner;
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
 * 注意：不使用分布式锁，因为都是从RPC获取最新数据，一致性有保证
 */
@Component
public class BannerScheduledTask {

    @Autowired
    private BannerService bannerService;

    @Autowired
    private BannerOperationClient bannerOperationClient;

    /**
     * 增量同步任务
     * 每5分钟执行一次，同步最近5分钟内更新的Banner
     * 任务名：incrementalSyncJob（需在XXL-Job管理后台配置）
     */
    @XxlJob("incrementalSyncJob")
    public void incrementalSync() {
        // Step1: 获取最近5分钟更新的Banner ID列表
        long sinceTime = System.currentTimeMillis() - 5 * 60 * 1000;
        List<Long> updatedIds = bannerOperationClient.getIncrementalUpdatedIds(sinceTime);

        // Step2: 逐个同步到Redis
        for (Long bannerId : updatedIds) {
            bannerService.syncBanner(bannerId);
        }
    }

    /**
     * 全量一致性检查任务
     * 每30分钟执行一次，检查所有Banner数据与上游是否一致
     * 任务名：fullConsistencyCheckJob（需在XXL-Job管理后台配置）
     */
    @XxlJob("fullConsistencyCheckJob")
    public void fullConsistencyCheck() {
        // Step1: 获取所有活跃Banner ID
        List<Long> allIds = bannerOperationClient.getAllActiveBannerIds();

        // Step2: 逐个检查一致性
        for (Long bannerId : allIds) {
            Banner data = bannerOperationClient.getBannerById(bannerId);
            if (Objects.nonNull(data)) {
                // 上游存在：同步Banner数据到Redis
                bannerService.syncBanner(bannerId);
            } else {
                // 上游不存在：设置为非活跃状态（软删除）
                bannerService.setBannerInactive(bannerId);
            }
        }
    }
}