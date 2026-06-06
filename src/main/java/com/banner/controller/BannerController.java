package com.banner.controller;

import com.banner.common.dto.ApiResponse;
import com.banner.common.dto.request.BannerQueryRequest;
import com.banner.common.dto.request.BannerSyncRequest;
import com.banner.common.dto.response.BannerQueryResponse;
import com.banner.service.BannerQueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/banners")
public class BannerController {

    @Autowired
    private BannerQueryService bannerQueryService;

    @GetMapping
    public ApiResponse<BannerQueryResponse> getBanners(BannerQueryRequest request) {
        List<BannerSyncRequest> banners = bannerQueryService.getBannersByProductDateAndUserId(
                request.getProductId(), request.getDate(), request.getUserId());

        List<BannerQueryResponse.BannerSimpleInfo> simpleInfos = banners.stream()
                .map(b -> new BannerQueryResponse.BannerSimpleInfo(
                        b.getId(),
                        b.getTitle(),
                        b.getImageUrl(),
                        b.getLinkUrl(),
                        b.getPriority()
                ))
                .collect(Collectors.toList());

        BannerQueryResponse response = new BannerQueryResponse(
                request.getProductId(), request.getDate(), simpleInfos);
        return ApiResponse.success(response);
    }
}
