package com.banner.application.service;

import com.banner.api.request.BannerQueryRequest;
import com.banner.api.response.BannerQueryResponse;

/**
 * Banner查询服务接口
 * <p>
 * 应用层服务，负责业务用例编排。
 * 调用领域层服务完成查询逻辑，不依赖具体实现。
 * </p>
 */
public interface BannerQueryService {

    /**
     * 查询Banner列表
     * <p>
     * 根据产品ID、日期、用户ID查询Banner列表，自动过滤人群包。
     * </p>
     *
     * @param request 查询请求参数
     * @return Banner列表响应
     */
    BannerQueryResponse queryBanners(BannerQueryRequest request);
}
