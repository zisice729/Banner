package com.banner.application.service.impl;

import com.banner.api.request.BannerQueryRequest;
import com.banner.api.response.BannerQueryResponse;
import com.banner.application.service.BannerQueryService;
import com.banner.common.util.DateUtil;
import com.banner.domain.model.Banner;
import com.banner.domain.service.BannerDomainService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Banner查询服务实现
 * <p>
 * 应用层服务实现，负责业务用例编排。
 * 调用领域层服务完成查询逻辑，将领域模型转换为响应DTO。
 * </p>
 */
@Service
public class BannerQueryServiceImpl implements BannerQueryService {

    @Autowired
    private BannerDomainService bannerDomainService;

    @Override
    public BannerQueryResponse queryBanners(BannerQueryRequest request) {
        String date = request.getDate();
        if (Objects.isNull(date) || date.trim().isEmpty()) {
            date = DateUtil.todayStr();
        }

        List<Banner> banners = bannerDomainService.queryActiveBannersByProductDateAndUserId(
                request.getProductId(), date, request.getUserId());

        List<BannerQueryResponse.BannerSimpleInfo> simpleInfos = banners.stream()
                .map(b -> new BannerQueryResponse.BannerSimpleInfo(
                        b.getId(),
                        b.getTitle(),
                        b.getImageUrl(),
                        b.getLinkUrl(),
                        b.getPriority()
                ))
                .collect(Collectors.toCollection(ArrayList::new));

        return new BannerQueryResponse(request.getProductId(), date, simpleInfos);
    }
}
