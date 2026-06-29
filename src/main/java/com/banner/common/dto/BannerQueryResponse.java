package com.banner.common.dto;

import java.util.List;

/**
 * Banner查询响应DTO
 * 包含产品ID、日期和Banner列表
 */
public class BannerQueryResponse {

    /**
     * 产品ID
     */
    private Integer productId;

    /**
     * 查询日期（格式：yyyyMMdd）
     */
    private String date;

    /**
     * Banner列表（已过滤人群包，按优先级降序排列）
     */
    private List<BannerSimpleInfo> banners;

    public BannerQueryResponse() {
    }

    public BannerQueryResponse(Integer productId, String date, List<BannerSimpleInfo> banners) {
        this.productId = productId;
        this.date = date;
        this.banners = banners;
    }

    public Integer getProductId() {
        return productId;
    }

    public void setProductId(Integer productId) {
        this.productId = productId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }

    public List<BannerSimpleInfo> getBanners() {
        return banners;
    }

    public void setBanners(List<BannerSimpleInfo> banners) {
        this.banners = banners;
    }

    /**
     * Banner简要信息
     * 用于对外返回，不包含敏感字段（如人群包用户列表）
     */
    public static class BannerSimpleInfo {
        /**
         * Banner ID
         */
        private Long id;

        /**
         * Banner标题
         */
        private String title;

        /**
         * Banner图片URL
         */
        private String imageUrl;

        /**
         * Banner跳转链接URL
         */
        private String linkUrl;

        /**
         * 优先级（数值越大优先级越高）
         */
        private Integer priority;

        public BannerSimpleInfo() {
        }

        public BannerSimpleInfo(Long id, String title, String imageUrl, String linkUrl, Integer priority) {
            this.id = id;
            this.title = title;
            this.imageUrl = imageUrl;
            this.linkUrl = linkUrl;
            this.priority = priority;
        }

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getTitle() {
            return title;
        }

        public void setTitle(String title) {
            this.title = title;
        }

        public String getImageUrl() {
            return imageUrl;
        }

        public void setImageUrl(String imageUrl) {
            this.imageUrl = imageUrl;
        }

        public String getLinkUrl() {
            return linkUrl;
        }

        public void setLinkUrl(String linkUrl) {
            this.linkUrl = linkUrl;
        }

        public Integer getPriority() {
            return priority;
        }

        public void setPriority(Integer priority) {
            this.priority = priority;
        }
    }
}