package com.banner.infrastructure.cache;

import com.banner.infrastructure.dto.BannerSyncRequest;

import java.util.List;

/**
 * Banner缓存管理器接口
 * <p>
 * 定义Banner缓存的操作接口，包括缓存读写、分桶管理等。
 * </p>
 */
public interface BannerCacheManager {

    void refreshBannerCache(Long id, BannerSyncRequest data);

    void deleteBannerCache(Long id);

    void clearUserBuckets(Long id);

    void writeUserBuckets(Long id, List<Long> userIds);

    BannerSyncRequest getBannerFromCache(Long id);

    List<Long> getUserIdsFromCache(Long id);

    boolean containsUserId(Long id, Long userId);

    List<BannerSyncRequest> getBannersByProductAndDate(Integer productId, String date);
}
