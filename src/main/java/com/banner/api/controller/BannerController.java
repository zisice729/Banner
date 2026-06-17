package com.banner.api.controller;

import com.banner.api.request.BannerQueryRequest;
import com.banner.api.response.ApiResponse;
import com.banner.api.response.BannerQueryResponse;
import com.banner.application.service.BannerQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Banner查询接口
 * <p>
 * 对外暴露HTTP接口，提供Banner列表查询功能。
 * Controller层只负责接收请求和返回响应，业务逻辑下沉到Service层。
 * </p>
 */
@RestController
@RequestMapping("/api/banners")
public class BannerController {

    @Autowired
    private BannerQueryService bannerQueryService;

    /**
     * 查询Banner列表
     * <p>
     * 根据产品ID、日期、用户ID查询Banner列表。
     * 返回的Banner列表已按优先级降序排序，且已过滤人群包。
     * </p>
     *
     * @param request 查询请求参数
     * @return Banner列表响应
     */
    @GetMapping
    public ApiResponse<BannerQueryResponse> getBanners(BannerQueryRequest request) {
        BannerQueryResponse response = bannerQueryService.queryBanners(request);
        return ApiResponse.success(response);
    }
}
