package com.banner.infrastructure.cache;

import com.banner.domain.model.Banner;
import com.banner.domain.repository.BannerRepository;
import com.banner.infrastructure.dto.BannerSyncRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Banner仓储实现
 * <p>
 * 基于Redis缓存实现Banner数据访问，实现领域层定义的仓储接口。
 * </p>
 */
@Component
public class BannerRepositoryImpl implements BannerRepository {

    @Autowired
    private BannerCacheManager bannerCacheManager;

    @Override
    public Banner findById(Long id) {
        BannerSyncRequest syncRequest = bannerCacheManager.getBannerFromCache(id);
        return convertToDomain(syncRequest);
    }

    @Override
    public List<Banner> findByProductAndDate(Integer productId, String date) {
        List<BannerSyncRequest> syncRequests = bannerCacheManager.getBannersByProductAndDate(productId, date);
        return syncRequests.stream()
                .map(this::convertToDomain)
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    @Override
    public boolean containsUserId(Long bannerId, Long userId) {
        return bannerCacheManager.containsUserId(bannerId, userId);
    }

    @Override
    public void save(Banner banner) {
        BannerSyncRequest syncRequest = convertToSyncRequest(banner);
        bannerCacheManager.refreshBannerCache(banner.getId(), syncRequest);
    }

    @Override
    public void delete(Long id) {
        bannerCacheManager.deleteBannerCache(id);
    }

    private Banner convertToDomain(BannerSyncRequest syncRequest) {
        if (syncRequest == null) {
            return null;
        }
        Banner banner = new Banner();
        banner.setId(syncRequest.getId());
        banner.setProductId(syncRequest.getProductId());
        banner.setTitle(syncRequest.getTitle());
        banner.setImageUrl(syncRequest.getImageUrl());
        banner.setLinkUrl(syncRequest.getLinkUrl());
        banner.setPriority(syncRequest.getPriority());
        banner.setStatus(syncRequest.getStatus());
        banner.setStartTime(syncRequest.getStartTime());
        banner.setEndTime(syncRequest.getEndTime());
        banner.setUserIds(syncRequest.getUserIds());
        return banner;
    }

    private BannerSyncRequest convertToSyncRequest(Banner banner) {
        BannerSyncRequest syncRequest = new BannerSyncRequest();
        syncRequest.setId(banner.getId());
        syncRequest.setProductId(banner.getProductId());
        syncRequest.setTitle(banner.getTitle());
        syncRequest.setImageUrl(banner.getImageUrl());
        syncRequest.setLinkUrl(banner.getLinkUrl());
        syncRequest.setPriority(banner.getPriority());
        syncRequest.setStatus(banner.getStatus());
        syncRequest.setStartTime(banner.getStartTime());
        syncRequest.setEndTime(banner.getEndTime());
        syncRequest.setUserIds(banner.getUserIds());
        return syncRequest;
    }
}
