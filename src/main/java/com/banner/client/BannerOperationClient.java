package com.banner.client;

import com.banner.common.dto.Banner;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * Banner运营端RPC客户端
 * 模拟调用上游运营系统获取Banner数据和用户列表
 * 实际项目中应替换为真实的RPC调用（如Dubbo、Feign等）
 */
@Component
public class BannerOperationClient {

    /**
     * 根据Banner ID获取Banner详情
     *
     * @param bannerId Banner ID
     * @return Banner对象
     */
    public Banner getBannerById(Long bannerId) {
        Banner data = new Banner();
        data.setId(bannerId);
        data.setProductId(1);
        data.setTitle("Test Banner");
        data.setImageUrl("https://example.com/image.jpg");
        data.setLinkUrl("https://example.com");
        data.setPriority(100);
        data.setStatus(1);
        data.setStartTime(System.currentTimeMillis());
        data.setEndTime(System.currentTimeMillis() + 86400000L);
        data.setBucketCount(1);
        return data;
    }

    /**
     * 分页获取Banner关联的用户ID列表
     *
     * @param bannerId Banner ID
     * @param page     页码（从0开始）
     * @param size     每页大小
     * @return 用户ID列表
     */
    public List<Long> getUserIdsByPage(Long bannerId, Integer page, Integer size) {
        List<Long> userIds = new ArrayList<>();
        for (int i = 0; i < size && i < 100; i++) {
            userIds.add((long) (page * size + i));
        }
        return userIds;
    }

    /**
     * 获取增量更新的Banner ID列表
     * 获取指定时间戳之后更新的Banner ID
     *
     * @param sinceTime 时间戳（毫秒）
     * @return 更新的Banner ID列表
     */
    public List<Long> getIncrementalUpdatedIds(long sinceTime) {
        return new ArrayList<>();
    }

    /**
     * 获取所有活跃Banner ID
     *
     * @return 活跃Banner ID列表
     */
    public List<Long> getAllActiveBannerIds() {
        return new ArrayList<>();
    }
}