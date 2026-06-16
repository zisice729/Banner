package com.banner.controller;

import com.banner.common.dto.ApiResponse;
import com.banner.common.dto.BannerInfo;
import com.banner.common.dto.BannerListResponse;
import com.banner.common.dto.BannerQueryRequest;
import com.banner.common.util.DateUtil;
import com.banner.common.util.StringUtils;
import com.banner.service.BannerService;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Banner查询接口
 * <p>
 * 提供C端用户查询Banner列表的HTTP接口，支持按productId和date查询。
 * 数据从Redis缓存获取，保证高性能查询。
 * </p>
 */
@RestController
@RequestMapping("/api/banners")
public class BannersController {

    @Autowired
    private BannerService bannerService;

    /**
     * 查询Banner列表
     * <p>
     * 处理流程：
     * 1. 获取查询参数productId和date
     * 2. 日期为空时默认使用当天日期
     * 3. 从Redis缓存中查询Banner列表
     * 4. 转换为响应DTO（只返回必要字段）
     * </p>
     */
    @GetMapping
    public ApiResponse<BannerListResponse> getBanners(@Valid @ModelAttribute BannerQueryRequest request) {
        String productId = request.getProductId();
        String date = request.getDate();

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
