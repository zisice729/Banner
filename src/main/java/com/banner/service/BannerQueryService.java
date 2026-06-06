package com.banner.service;

import com.banner.common.dto.request.BannerSyncRequest;

import java.util.List;

public interface BannerQueryService {

    List<BannerSyncRequest> getBannersByProductAndDate(Integer productId, String date);

    List<BannerSyncRequest> getBannersByProductDateAndUserId(Integer productId, String date, Long userId);
}
