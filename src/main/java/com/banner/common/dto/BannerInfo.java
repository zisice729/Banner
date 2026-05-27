package com.banner.common.dto;

import java.util.List;

public class BannerInfo {

    private String bannerId;
    private Integer bucketIndex;
    private Integer totalBuckets;
    private List<Long> userIds;
    private Long version;
    private String changeType;

    public BannerInfo() {
    }

    public String getBannerId() {
        return bannerId;
    }

    public void setBannerId(String bannerId) {
        this.bannerId = bannerId;
    }

    public Integer getBucketIndex() {
        return bucketIndex;
    }

    public void setBucketIndex(Integer bucketIndex) {
        this.bucketIndex = bucketIndex;
    }

    public Integer getTotalBuckets() {
        return totalBuckets;
    }

    public void setTotalBuckets(Integer totalBuckets) {
        this.totalBuckets = totalBuckets;
    }

    public List<Long> getUserIds() {
        return userIds;
    }

    public void setUserIds(List<Long> userIds) {
        this.userIds = userIds;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public String getChangeType() {
        return changeType;
    }

    public void setChangeType(String changeType) {
        this.changeType = changeType;
    }
}
