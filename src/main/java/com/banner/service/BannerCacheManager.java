package com.banner.service;

import com.banner.common.dto.BannerInfo;

import java.util.List;

/**
 * Banner缓存管理接口
 */
public interface BannerCacheManager {

    /**
     * 设置单个Banner到缓存
     */
    void setBanner(String productId, String date, BannerInfo bannerInfo);

    /**
     * 从缓存删除单个Banner
     */
    void deleteBanner(String productId, String date, String bannerId);

    /**
     * 从缓存获取指定日期的Banner列表
     */
    List<BannerInfo> getBannersByDate(String productId, String date);

    /**
     * 全量刷新指定日期的Banner缓存
     */
    void refreshBannersByDate(String productId, String date, List<BannerInfo> banners);
}
