package com.banner.common.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BucketCalculator {

    private BucketCalculator() {
    }

    public static int calcBucketIndex(Long userId, int totalBuckets) {
        return (int) (userId % totalBuckets);
    }

    public static int calcTotalBuckets(int userCount, int bucketSize) {
        if (userCount <= 0) {
            return 0;
        }
        return (int) Math.ceil((double) userCount / bucketSize);
    }

    public static Map<Integer, List<Long>> distributeToBuckets(List<Long> userIds, int totalBuckets) {
        Map<Integer, List<Long>> bucketMap = new HashMap<>();
        if (userIds == null || userIds.isEmpty() || totalBuckets <= 0) {
            return bucketMap;
        }
        for (Long userId : userIds) {
            int bucketIndex = calcBucketIndex(userId, totalBuckets);
            bucketMap.computeIfAbsent(bucketIndex, k -> new ArrayList<>()).add(userId);
        }
        return bucketMap;
    }
}
