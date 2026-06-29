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
 * 接口路径：/api/banners
 */
@RestController
@RequestMapping("/api/banners")
public class BannerController {

    @Autowired
    private BannerService bannerService;

    /**
     * 查询Banner列表
     * 根据产品ID和用户ID筛选出用户可见的Banner
     * 日期由系统内部获取（当前日期），无需前端传递
     *
     * @param request 请求体，包含productId（产品ID）和userId（用户ID）
     * @return ApiResponse<BannerQueryResponse> 包含产品ID、日期和Banner列表
     */
    @PostMapping
    public ApiResponse<BannerQueryResponse> getBanners(BannerQueryRequest request) {
        BannerQueryResponse response = bannerService.queryBanners(
                request.getProductId(),
                request.getUserId()
        );
        return ApiResponse.success(response);
    }
}