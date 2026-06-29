package com.banner.common.dto;

/**
 * Banner查询请求DTO
 * 包含产品ID和用户ID，日期由系统内部获取（当前日期），无需前端传递
 */
public class BannerQueryRequest {

    /**
     * 产品ID
     */
    private Integer productId;

    /**
     * 用户ID
     */
    private Long userId;

    public BannerQueryRequest() {
    }

    public Integer getProductId() {
        return productId;
    }

    public void setProductId(Integer productId) {
        this.productId = productId;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }
}