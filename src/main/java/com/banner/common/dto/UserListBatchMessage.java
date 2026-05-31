package com.banner.common.dto;

import java.util.List;

public class UserListBatchMessage {

    private String bannerId;
    private Long version;
    private Integer batchIndex;
    private Integer totalBatches;
    private Integer totalBuckets;
    private List<Long> userIds;

    public UserListBatchMessage() {
    }

    public String getBannerId() {
        return bannerId;
    }

    public void setBannerId(String bannerId) {
        this.bannerId = bannerId;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public Integer getBatchIndex() {
        return batchIndex;
    }

    public void setBatchIndex(Integer batchIndex) {
        this.batchIndex = batchIndex;
    }

    public Integer getTotalBatches() {
        return totalBatches;
    }

    public void setTotalBatches(Integer totalBatches) {
        this.totalBatches = totalBatches;
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
}
