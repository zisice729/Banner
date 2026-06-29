package com.banner.common.dto;

import java.util.List;

/**
 * Banner领域模型
 * 包含Banner基本信息和人群包配置
 */
public class Banner {

    /**
     * Banner ID
     */
    private Long id;

    /**
     * 产品ID
     */
    private Integer productId;

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

    /**
     * 状态：1-活跃，0-不活跃（软删除）
     */
    private Integer status;

    /**
     * 开始时间（时间戳，毫秒）
     */
    private Long startTime;

    /**
     * 结束时间（时间戳，毫秒）
     */
    private Long endTime;

    /**
     * 桶数量（本地计算得出，用于人群包分桶）
     */
    private Integer bucketCount;

    /**
     * 用户ID列表（RPC获取，用于分桶计算）
     */
    private List<Long> userIds;

    public Banner() {
    }

    /**
     * 判断Banner是否活跃
     * status=1表示活跃
     *
     * @return true-活跃，false-不活跃
     */
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

    public Integer getBucketCount() {
        return bucketCount;
    }

    public void setBucketCount(Integer bucketCount) {
        this.bucketCount = bucketCount;
    }

    public List<Long> getUserIds() {
        return userIds;
    }

    public void setUserIds(List<Long> userIds) {
        this.userIds = userIds;
    }
}