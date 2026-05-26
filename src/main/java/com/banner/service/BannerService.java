package com.banner.service;

import com.banner.common.dto.BannerInfo;

import java.util.List;

/**
 * Banner业务逻辑接口
 */
public interface BannerService {

    /**
     * 获取指定日期的Banner列表
     */
    List<BannerInfo> getBannersByDate(String productId, String date);

    /**
     * 从Kafka同步Banner数据
     */
    void syncBannerFromKafka(BannerInfo bannerInfo);

    /**
     * 从数据库刷新Banner数据
     */
    void refreshFromDatabase(String productId, String date);
}
