package com.banner.service.impl;

import com.banner.common.dto.request.BannerSyncRequest;
import com.banner.common.util.DateUtil;
import com.banner.service.BannerCacheManager;
import com.banner.service.BannerQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
public class BannerQueryServiceImpl implements BannerQueryService {

    @Autowired
    private BannerCacheManager bannerCacheManager;

    @Override
    public List<BannerSyncRequest> getBannersByProductAndDate(Integer productId, String date) {
        if (Objects.isNull(date) || date.trim().isEmpty()) {
            date = DateUtil.todayStr();
        }
        return bannerCacheManager.getBannersByProductAndDate(productId, date);
    }

    @Override
    public List<BannerSyncRequest> getBannersByProductDateAndUserId(Integer productId, String date, Long userId) {
        if (Objects.isNull(date) || date.trim().isEmpty()) {
            date = DateUtil.todayStr();
        }

        List<BannerSyncRequest> allBanners = bannerCacheManager.getBannersByProductAndDate(productId, date);

        return allBanners.stream()
                .filter(banner -> bannerCacheManager.containsUserId(banner.getId(), userId))
                .collect(Collectors.toList());
    }
}
