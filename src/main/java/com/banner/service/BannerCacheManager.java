package com.banner.service;

import com.banner.common.dto.SimpleBannerInfo;

import java.util.List;

public interface BannerCacheManager {

    void setBanner(String productId, String date, SimpleBannerInfo bannerInfo);

    void deleteBanner(String productId, String date, String bannerId);

    List<SimpleBannerInfo> getBannersByDate(String productId, String date);

    void refreshBannersByDate(String productId, String date, List<SimpleBannerInfo> banners);
}
