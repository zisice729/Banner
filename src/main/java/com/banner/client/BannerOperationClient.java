package com.banner.client;

import com.banner.infrastructure.dto.BannerSyncRequest;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class BannerOperationClient {

    public BannerSyncRequest getBannerById(Long id) {
        BannerSyncRequest data = new BannerSyncRequest();
        data.setId(id);
        data.setProductId(1);
        data.setTitle("Test Banner");
        data.setImageUrl("https://example.com/image.jpg");
        data.setLinkUrl("https://example.com");
        data.setPriority(100);
        data.setStatus(1);
        data.setStartTime(System.currentTimeMillis());
        data.setEndTime(System.currentTimeMillis() + 86400000L);
        return data;
    }

    public List<Long> getUserIdsByPage(Long bannerId, Integer page, Integer size) {
        List<Long> userIds = new ArrayList<>();
        for (int i = 0; i < size && i < 100; i++) {
            userIds.add((long) (page * size + i));
        }
        return userIds;
    }

    public List<Long> getIncrementalUpdatedIds(long sinceTime) {
        return new ArrayList<>();
    }

    public List<Long> getAllActiveBannerIds() {
        return new ArrayList<>();
    }
}
