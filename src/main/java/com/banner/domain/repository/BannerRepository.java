package com.banner.domain.repository;

import com.banner.domain.model.Banner;

import java.util.List;

/**
 * Banner仓储接口
 * <p>
 * 定义Banner数据访问的抽象接口，领域层依赖此接口，不依赖具体实现。
 * 由基础设施层提供实现（Redis缓存）。
 * </p>
 */
public interface BannerRepository {

    Banner findById(Long id);

    List<Banner> findByProductAndDate(Integer productId, String date);

    boolean containsUserId(Long bannerId, Long userId);

    void save(Banner banner);

    void delete(Long id);
}
