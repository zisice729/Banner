package com.banner.service;

import com.banner.common.dto.SimpleBannerInfo;

import java.util.List;

public interface BannerQueryService {

    List<SimpleBannerInfo> getBannersByDate(String productId, String date);

    List<SimpleBannerInfo> getBannersByDateAndUserId(String productId, String date, Long userId);
}
