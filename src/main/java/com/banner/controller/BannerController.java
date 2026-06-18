package com.banner.controller;

import com.banner.common.dto.ApiResponse;
import com.banner.common.dto.BannerQueryRequest;
import com.banner.common.dto.BannerQueryResponse;
import com.banner.service.BannerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Banner控制器
 * 对外HTTP接口层，接收请求并调用Service层
 */
@RestController
@RequestMapping("/api/banners")
public class BannerController {

    @Autowired
    private BannerService bannerService;

    /**
     * 查询Banner列表
     * 根据产品ID、日期、用户ID筛选出用户可见的Banner
     * POST /api/banners
     */
    @PostMapping
    public ApiResponse<BannerQueryResponse> getBanners(BannerQueryRequest request) {
        BannerQueryResponse response = bannerService.queryBanners(
                request.getProductId(),
                request.getDate(),
                request.getUserId()
        );
        return ApiResponse.success(response);
    }
}
