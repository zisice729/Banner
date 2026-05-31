package com.banner.service.impl;

import com.banner.common.dto.SimpleBannerInfo;
import com.banner.common.util.DateUtil;
import com.banner.common.util.StringUtils;
import com.banner.service.BannerCacheManager;
import com.banner.service.BannerQueryService;
import com.banner.service.UserBucketCacheManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class BannerQueryServiceImpl implements BannerQueryService {

    @Autowired
    private BannerCacheManager bannerCacheManager;

    @Autowired
    private UserBucketCacheManager userBucketCacheManager;

    @Override
    public List<SimpleBannerInfo> getBannersByDate(String productId, String date) {
        if (StringUtils.isEmpty(date)) {
            date = DateUtil.todayStr();
        }
        return bannerCacheManager.getBannersByDate(productId, date);
    }

    @Override
    public List<SimpleBannerInfo> getBannersByDateAndUserId(String productId, String date, Long userId) {
        if (StringUtils.isEmpty(date)) {
            date = DateUtil.todayStr();
        }

        List<SimpleBannerInfo> allBanners = bannerCacheManager.getBannersByDate(productId, date);

        if (userId == null) {
            return allBanners;
        }

        return allBanners.stream()
                .filter(banner -> {
                    if (!userBucketCacheManager.hasBucket(banner.getBannerId())) {
                        return true;
                    }
                    return checkUserInBanner(banner.getBannerId(), userId);
                })
                .collect(Collectors.toList());
    }

    private boolean checkUserInBanner(String bannerId, Long userId) {
        Integer totalBuckets = userBucketCacheManager.getTotalBuckets(bannerId);
        if (totalBuckets == null || totalBuckets <= 0) {
            return true;
        }
        int bucketIndex = (int) (userId % totalBuckets);
        return userBucketCacheManager.isUserInBucket(bannerId, bucketIndex, userId);
    }
}
