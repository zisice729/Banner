package com.banner.service;

import java.util.List;

public interface BannerUserListService {

    void syncUserList(String bannerId, int totalBuckets, List<Long> userIds, Long version);

    void syncUserListBatch(String bannerId, Long version, int batchIndex, int totalBatches, int totalBuckets, List<Long> userIds);

    void deleteUserList(String bannerId);

    void refreshFromDatabase(String bannerId);
}
