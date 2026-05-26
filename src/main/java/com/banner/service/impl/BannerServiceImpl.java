package com.banner.service.impl;

import com.banner.common.dto.BannerInfo;
import com.banner.common.entity.Banner;
import com.banner.common.util.DateUtil;
import com.banner.common.util.StringUtils;
import com.banner.convert.BannerConvert;
import com.banner.lock.RedisDistributedLock;
import com.banner.repository.BannerRepository;
import com.banner.service.BannerCacheManager;
import com.banner.service.BannerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Banner业务逻辑实现类
 */
@Service
public class BannerServiceImpl implements BannerService {

    @Autowired
    private BannerRepository bannerRepository;

    @Autowired
    private BannerCacheManager bannerCacheManager;

    @Autowired
    private RedisDistributedLock redisDistributedLock;

    @Autowired
    private BannerConvert bannerConvert;

    @Override
    public List<BannerInfo> getBannersByDate(String productId, String date) {
        if (StringUtils.isEmpty(date)) {
            date = DateUtil.todayStr();
        }
        return bannerCacheManager.getBannersByDate(productId, date);
    }

    /**
     * 从Kafka同步Banner数据
     */
    @Override
    @Transactional
    public void syncBannerFromKafka(BannerInfo bannerInfo) {
        try {
            List<String> dates = DateUtil.getDateRange(bannerInfo.getStartDay(), bannerInfo.getEndDay());

            for (String date : dates) {
                boolean locked = redisDistributedLock.tryLockWithRetry(bannerInfo.getProductId(), date);
                if (!locked) {
                    continue;
                }

                try {
                    if ("DELETE".equals(bannerInfo.getChangeType())) {
                        bannerCacheManager.deleteBanner(bannerInfo.getProductId(), date, bannerInfo.getBannerId());
                    } else {
                        bannerCacheManager.setBanner(bannerInfo.getProductId(), date, bannerInfo);
                    }
                } finally {
                    redisDistributedLock.unlock(bannerInfo.getProductId(), date);
                }
            }
        } catch (Exception e) {
        }
    }

    /**
     * 从数据库刷新Banner数据
     */
    @Override
    @Transactional
    public void refreshFromDatabase(String productId, String date) {
        LocalDate localDate = DateUtil.parseYYYYMMDD(date);
        List<Banner> banners = bannerRepository.findByProductIdAndDate(
                productId, localDate
        );

        List<BannerInfo> bannerInfos = banners.stream()
                .map(bannerConvert::toBannerInfo)
                .collect(Collectors.toList());

        bannerCacheManager.refreshBannersByDate(productId, date, bannerInfos);
    }
}
