package com.banner.service;

import com.banner.common.dto.SimpleBannerInfo;

public interface BannerSyncService {

    void syncBannerFromKafka(SimpleBannerInfo bannerInfo);

    void deleteBannerFromKafka(SimpleBannerInfo bannerInfo);

    void refreshFromDatabase(String productId, String date);
}
