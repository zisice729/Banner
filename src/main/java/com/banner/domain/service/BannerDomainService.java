package com.banner.domain.service;

import com.banner.domain.model.Banner;
import com.banner.domain.repository.BannerRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Banner领域服务
 * <p>
 * 处理Banner核心业务逻辑，包括查询、人群包过滤等。
 * 依赖仓储接口，不依赖具体实现。
 * </p>
 */
@Service
public class BannerDomainService {

    @Autowired
    private BannerRepository bannerRepository;

    public List<Banner> queryActiveBannersByProductDateAndUserId(Integer productId, String date, Long userId) {
        List<Banner> banners = bannerRepository.findByProductAndDate(productId, date);

        return banners.stream()
                .filter(Banner::isActive)
                .filter(banner -> bannerRepository.containsUserId(banner.getId(), userId))
                .sorted((a, b) -> {
                    if (a.getPriority() == null) return 1;
                    if (b.getPriority() == null) return -1;
                    return b.getPriority().compareTo(a.getPriority());
                })
                .collect(Collectors.toList());
    }
}
