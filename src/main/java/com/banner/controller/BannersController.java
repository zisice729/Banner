package com.banner.controller;

import com.banner.common.dto.ApiResponse;
import com.banner.common.dto.BannerListResponse;
import com.banner.common.dto.SimpleBannerInfo;
import com.banner.common.util.DateUtil;
import com.banner.common.util.StringUtils;
import com.banner.service.BannerQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/banners")
public class BannersController {

    @Autowired
    private BannerQueryService bannerQueryService;

    @GetMapping
    public ApiResponse<BannerListResponse> getBanners(
            @RequestParam String productId,
            @RequestParam(required = false) String date,
            @RequestParam(required = false) Long userId) {

        if (StringUtils.isEmpty(date)) {
            date = DateUtil.todayStr();
        }

        List<SimpleBannerInfo> banners;
        if (userId != null) {
            banners = bannerQueryService.getBannersByDateAndUserId(productId, date, userId);
        } else {
            banners = bannerQueryService.getBannersByDate(productId, date);
        }

        List<BannerListResponse.BannerSimpleInfo> simpleInfos = banners.stream()
                .map(b -> new BannerListResponse.BannerSimpleInfo(
                        b.getBannerId(),
                        b.getTitle(),
                        b.getImageUrl(),
                        b.getLinkUrl(),
                        b.getPriority()
                ))
                .collect(Collectors.toList());

        BannerListResponse response = new BannerListResponse(productId, date, simpleInfos);
        return ApiResponse.success(response);
    }
}
