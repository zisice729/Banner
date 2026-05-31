package com.banner.service.impl;

import com.banner.common.dto.SimpleBannerInfo;
import com.banner.common.entity.SimpleBanner;
import com.banner.common.util.DateUtil;
import com.banner.common.util.RedisKeyBuilder;
import com.banner.convert.BannerConvert;
import com.banner.lock.RedisDistributedLock;
import com.banner.repository.SimpleBannerRepository;
import com.banner.service.BannerCacheManager;
import com.banner.service.BannerSyncService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class BannerSyncServiceImpl implements BannerSyncService {

    private static final Logger log = LoggerFactory.getLogger(BannerSyncServiceImpl.class);

    @Autowired
    private SimpleBannerRepository simpleBannerRepository;

    @Autowired
    private BannerCacheManager bannerCacheManager;

    @Autowired
    private RedisDistributedLock redisDistributedLock;

    @Autowired
    private BannerConvert bannerConvert;

    @Override
    public void syncBannerFromKafka(SimpleBannerInfo bannerInfo) {
        List<String> dates = DateUtil.getDateRange(
                DateUtil.parseYYYYMMDD(bannerInfo.getStartDay()),
                DateUtil.parseYYYYMMDD(bannerInfo.getEndDay())
        );

        List<String> lockKeys = new ArrayList<>();
        for (String date : dates) {
            lockKeys.add(RedisKeyBuilder.bannerLock(bannerInfo.getProductId(), date));
        }

        Map<String, String> locks = redisDistributedLock.tryLockAll(lockKeys);
        if (locks == null) {
            log.error("syncBannerFromKafka failed to acquire all locks, productId={}", bannerInfo.getProductId());
            return;
        }

        try {
            for (String date : dates) {
                bannerCacheManager.setBanner(bannerInfo.getProductId(), date, bannerInfo);
            }
        } finally {
            redisDistributedLock.unlockAll(locks);
        }
    }

    @Override
    public void deleteBannerFromKafka(SimpleBannerInfo bannerInfo) {
        List<String> dates = DateUtil.getDateRange(
                DateUtil.parseYYYYMMDD(bannerInfo.getStartDay()),
                DateUtil.parseYYYYMMDD(bannerInfo.getEndDay())
        );

        List<String> lockKeys = new ArrayList<>();
        for (String date : dates) {
            lockKeys.add(RedisKeyBuilder.bannerLock(bannerInfo.getProductId(), date));
        }

        Map<String, String> locks = redisDistributedLock.tryLockAll(lockKeys);
        if (locks == null) {
            log.error("deleteBannerFromKafka failed to acquire all locks, productId={}", bannerInfo.getProductId());
            return;
        }

        try {
            for (String date : dates) {
                bannerCacheManager.deleteBanner(bannerInfo.getProductId(), date, bannerInfo.getBannerId());
            }
        } finally {
            redisDistributedLock.unlockAll(locks);
        }
    }

    @Override
    public void refreshFromDatabase(String productId, String date) {
        LocalDate localDate = DateUtil.parseYYYYMMDD(date);
        List<SimpleBanner> banners = simpleBannerRepository.findByProductIdAndDate(productId, localDate);

        List<SimpleBannerInfo> bannerInfos = banners.stream()
                .map(bannerConvert::toSimpleBannerInfo)
                .toList();

        bannerCacheManager.refreshBannersByDate(productId, date, bannerInfos);
    }
}
