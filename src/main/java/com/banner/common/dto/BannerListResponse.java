package com.banner.common.dto;

import java.util.List;

public class BannerListResponse {

    private String productId;
    private String date;
    private List<BannerSimpleInfo> banners;

    public BannerListResponse() {
    }

    public BannerListResponse(String productId, String date, List<BannerSimpleInfo> banners) {
        this.productId = productId;
        this.date = date;
        this.banners = banners;
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
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

    public static class BannerSimpleInfo {
        private String bannerId;
        private String title;
        private String imageUrl;
        private String linkUrl;
        private Integer priority;

        public BannerSimpleInfo() {
        }

        public BannerSimpleInfo(String bannerId, String title, String imageUrl, String linkUrl, Integer priority) {
            this.bannerId = bannerId;
            this.title = title;
            this.imageUrl = imageUrl;
            this.linkUrl = linkUrl;
            this.priority = priority;
        }

        public String getBannerId() {
            return bannerId;
        }

        public void setBannerId(String bannerId) {
            this.bannerId = bannerId;
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
