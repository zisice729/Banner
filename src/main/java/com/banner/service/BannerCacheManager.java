package com.banner.service;

import com.banner.common.dto.request.BannerSyncRequest;

import java.util.List;

public interface BannerCacheManager {

    void refreshBannerCache(Long id, BannerSyncRequest data);

    void deleteBannerCache(Long id);

    BannerSyncRequest getBannerFromCache(Long id);

    List<Long> getUserIdsFromCache(Long id);

    boolean containsUserId(Long id, Long userId);

    List<BannerSyncRequest> getBannersByProductAndDate(Integer productId, String date);
}
