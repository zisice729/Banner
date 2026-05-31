package com.banner.service.impl;

import com.banner.common.constant.BannerConstants;
import com.banner.common.util.BucketCalculator;
import com.banner.repository.BannerUserRepository;
import com.banner.service.BannerUserListService;
import com.banner.service.UserBucketCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
public class BannerUserListServiceImpl implements BannerUserListService {

    private static final Logger log = LoggerFactory.getLogger(BannerUserListServiceImpl.class);

    @Autowired
    private BannerUserRepository bannerUserRepository;

    @Autowired
    private UserBucketCacheManager userBucketCacheManager;

    @Override
    public void syncUserList(String bannerId, int totalBuckets, List<Long> userIds, Long version) {
        Long currentVersion = userBucketCacheManager.getVersion(bannerId);
        if (currentVersion == null) {
            currentVersion = 0L;
        }

        if (version != null && version <= currentVersion) {
            return;
        }

        if (userIds == null || userIds.isEmpty()) {
            return;
        }

        Map<Integer, List<Long>> bucketData = BucketCalculator.distributeToBuckets(userIds, totalBuckets);
        userBucketCacheManager.atomicSwitch(bannerId, totalBuckets, bucketData, version);

        log.info("syncUserList completed, bannerId={}, version={}, totalBuckets={}, userCount={}",
                bannerId, version, totalBuckets, userIds.size());
    }

    @Override
    public void syncUserListBatch(String bannerId, Long version, int batchIndex, int totalBatches, int totalBuckets, List<Long> userIds) {
        Long currentVersion = userBucketCacheManager.getVersion(bannerId);
        if (currentVersion == null) {
            currentVersion = 0L;
        }

        if (version != null && version <= currentVersion) {
            log.debug("syncUserListBatch skipped, version too old, bannerId={}, version={}, currentVersion={}",
                    bannerId, version, currentVersion);
            return;
        }

        userBucketCacheManager.setTempBatchData(bannerId, version, batchIndex, userIds);
        userBucketCacheManager.updateTempBatchMeta(bannerId, version, batchIndex, totalBatches);

        if (userBucketCacheManager.isAllBatchesReceived(bannerId, version, totalBatches)) {
            userBucketCacheManager.mergeAndSwitch(bannerId, version, totalBuckets);
            log.info("syncUserListBatch all batches received and merged, bannerId={}, version={}, totalBatches={}",
                    bannerId, version, totalBatches);
        } else {
            log.debug("syncUserListBatch batch received, waiting for more, bannerId={}, version={}, batchIndex={}/{}",
                    bannerId, version, batchIndex + 1, totalBatches);
        }
    }

    @Override
    public void deleteUserList(String bannerId) {
        userBucketCacheManager.deleteAllBuckets(bannerId);
        log.info("deleteUserList completed, bannerId={}", bannerId);
    }

    @Override
    public void refreshFromDatabase(String bannerId) {
        List<Long> userIds = bannerUserRepository.findUserIdsByBannerId(bannerId);

        if (userIds.isEmpty()) {
            userBucketCacheManager.deleteAllBuckets(bannerId);
            return;
        }

        int totalBuckets = BucketCalculator.calcTotalBuckets(userIds.size(), BannerConstants.BUCKET_SIZE);
        Map<Integer, List<Long>> bucketData = BucketCalculator.distributeToBuckets(userIds, totalBuckets);
        userBucketCacheManager.atomicSwitch(bannerId, totalBuckets, bucketData, null);

        log.info("refreshFromDatabase completed, bannerId={}, totalBuckets={}, userCount={}",
                bannerId, totalBuckets, userIds.size());
    }
}
