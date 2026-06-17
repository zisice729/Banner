package com.banner.domain.model;

import java.util.List;

/**
 * Banner领域模型
 * <p>
 * 代表Banner的核心业务概念，包含Banner的所有业务属性和行为。
 * </p>
 */
public class Banner {

    private Long id;
    private Integer productId;
    private String title;
    private String imageUrl;
    private String linkUrl;
    private Integer priority;
    private Integer status;
    private Long startTime;
    private Long endTime;
    private List<Long> userIds;

    public Banner() {
    }

    public Banner(Long id, Integer productId, String title, String imageUrl, String linkUrl,
                  Integer priority, Integer status, Long startTime, Long endTime) {
        this.id = id;
        this.productId = productId;
        this.title = title;
        this.imageUrl = imageUrl;
        this.linkUrl = linkUrl;
        this.priority = priority;
        this.status = status;
        this.startTime = startTime;
        this.endTime = endTime;
    }

    public boolean isActive() {
        return status != null && status == 1;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Integer getProductId() {
        return productId;
    }

    public void setProductId(Integer productId) {
        this.productId = productId;
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

    public Integer getStatus() {
        return status;
    }

    public void setStatus(Integer status) {
        this.status = status;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public List<Long> getUserIds() {
        return userIds;
    }

    public void setUserIds(List<Long> userIds) {
        this.userIds = userIds;
    }
}
