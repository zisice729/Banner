package com.banner.controller;

import com.banner.common.dto.ApiResponse;
import com.banner.common.dto.BannerInfo;
import com.banner.common.dto.BannerListResponse;
import com.banner.common.util.DateUtil;
import com.banner.common.util.StringUtils;
import com.banner.service.BannerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Banner查询接口
 */
@RestController
@RequestMapping("/api/banners")
public class BannersController {

    @Autowired
    private BannerService bannerService;

    @GetMapping
    public ApiResponse<BannerListResponse> getBanners(
            @RequestParam String productId,
            @RequestParam(required = false) String date) {

        if (StringUtils.isEmpty(date)) {
            date = DateUtil.todayStr();
        }

        List<BannerInfo> banners = bannerService.getBannersByDate(productId, date);

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
