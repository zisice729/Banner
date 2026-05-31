package com.banner.service;

import java.util.List;
import java.util.Map;

public interface UserBucketCacheManager {

    void setBucket(String bannerId, int bucketIndex, List<Long> userIds);

    void deleteAllBuckets(String bannerId);

    boolean hasBucket(String bannerId);

    boolean isUserInBucket(String bannerId, int bucketIndex, Long userId);

    Integer getTotalBuckets(String bannerId);

    void setTotalBuckets(String bannerId, int totalBuckets);

    Long getVersion(String bannerId);

    void setVersion(String bannerId, Long version);

    void atomicSwitch(String bannerId, int totalBuckets, Map<Integer, List<Long>> bucketData, Long version);

    void setTempBatchData(String bannerId, Long version, int batchIndex, List<Long> userIds);

    void updateTempBatchMeta(String bannerId, Long version, int batchIndex, int totalBatches);

    TempBatchMeta getTempBatchMeta(String bannerId, Long version);

    boolean isAllBatchesReceived(String bannerId, Long version, int totalBatches);

    void mergeAndSwitch(String bannerId, Long version, int totalBuckets);

    void cleanTempData(String bannerId, Long version);

    void cleanExpiredTempData(int expireMinutes);

    class TempBatchMeta {
        private Long version;
        private int totalBatches;
        private List<Integer> receivedBatchIndices;
        private long createTime;

        public TempBatchMeta() {
        }

        public TempBatchMeta(Long version, int totalBatches, List<Integer> receivedBatchIndices, long createTime) {
            this.version = version;
            this.totalBatches = totalBatches;
            this.receivedBatchIndices = receivedBatchIndices;
            this.createTime = createTime;
        }

        public Long getVersion() {
            return version;
        }

        public void setVersion(Long version) {
            this.version = version;
        }

        public int getTotalBatches() {
            return totalBatches;
        }

        public void setTotalBatches(int totalBatches) {
            this.totalBatches = totalBatches;
        }

        public List<Integer> getReceivedBatchIndices() {
            return receivedBatchIndices;
        }

        public void setReceivedBatchIndices(List<Integer> receivedBatchIndices) {
            this.receivedBatchIndices = receivedBatchIndices;
        }

        public long getCreateTime() {
            return createTime;
        }

        public void setCreateTime(long createTime) {
            this.createTime = createTime;
        }
    }
}
