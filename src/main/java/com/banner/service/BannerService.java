package com.banner.service;

import com.banner.client.BannerOperationClient;
import com.banner.common.constant.BannerConstants;
import com.banner.common.dto.Banner;
import com.banner.common.dto.BannerQueryResponse;
import com.banner.common.util.DateUtil;
import com.banner.common.util.JsonUtil;
import com.github.benmanes.caffeine.cache.Cache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Banner服务
 * 职责：Banner查询、数据同步、人群包分桶管理
 * 数据存储：Redis + Caffeine本地缓存（30秒TTL自动失效）
 */
@Service
public class BannerService {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private Cache<String, Banner> bannerLocalCache;

    @Autowired
    private Cache<String, Set<Object>> productDateLocalCache;

    @Autowired
    private BannerOperationClient bannerOperationClient;

    /**
     * 查询Banner列表
     * 根据产品ID、日期、用户ID筛选出用户可见的Banner
     */
    public BannerQueryResponse queryBanners(Integer productId, String date, Long userId) {
        // 1. 根据产品+日期获取所有活跃Banner（过滤status=1）
        List<Banner> banners = getActiveBanners(productId, date);

        // 2. 过滤人群包：检查用户是否在Banner的人群包中
        List<Banner> matchedBanners = banners.stream()
                .filter(banner -> isUserInBucket(banner.getId(), userId))
                .collect(Collectors.toList());

        // 3. 转换为响应DTO，按优先级降序排列
        List<BannerQueryResponse.BannerSimpleInfo> simpleInfos = matchedBanners.stream()
                .map(b -> new BannerQueryResponse.BannerSimpleInfo(
                        b.getId(), b.getTitle(), b.getImageUrl(), b.getLinkUrl(), b.getPriority()))
                .collect(Collectors.toCollection(ArrayList::new));

        return new BannerQueryResponse(productId, date, simpleInfos);
    }

    /**
     * 同步Banner数据
     * 从RPC获取Banner信息和用户列表，写入Redis
     */
    public void syncBanner(Long bannerId) {
        // 1. RPC获取Banner基本信息
        Banner banner = bannerOperationClient.getBannerById(bannerId);
        if (Objects.isNull(banner)) {
            throw new RuntimeException("RPC getBannerById return null, bannerId=" + bannerId);
        }

        // 2. 获取桶数量，默认1个桶
        Integer bucketCount = banner.getBucketCount();
        if (bucketCount == null || bucketCount <= 0) {
            bucketCount = 1;
        }

        // 3. 清除旧的用户桶数据
        deleteAllUserBuckets(bannerId, bucketCount);

        // 4. 分页RPC获取用户列表，边获取边写入Redis
        int page = 0;
        int maxPage = BannerConstants.MAX_RPC_PAGE_COUNT;
        while (page < maxPage) {
            List<Long> pageUserIds = bannerOperationClient.getUserIdsByPage(bannerId, page, BannerConstants.USER_BUCKET_SIZE);
            if (Objects.isNull(pageUserIds) || pageUserIds.isEmpty()) {
                break;
            }
            // 根据bucketCount动态计算桶索引，按userId取余分配
            addUserToBuckets(bannerId, bucketCount, pageUserIds);
            page++;
        }

        // 5. 超过最大页数限制，抛异常（防止无限循环）
        if (page >= maxPage) {
            throw new RuntimeException("Exceed max page limit, bannerId=" + bannerId);
        }

        // 6. 保存Banner详情到Redis（key: bannerInfo:bannerId:{id}）
        // 保存日期索引到Redis（key: banners:productId:{productId}:date:{date}）
        saveBannerWithIndex(banner);
    }

    /**
     * 设置Banner为非活跃状态（软删除）
     * 不删除人群包数据，只设置status=0
     */
    public void setBannerInactive(Long bannerId) {
        String bannerKey = String.format("bannerInfo:bannerId:%d", bannerId);
        Banner banner = getBanner(bannerId);
        if (Objects.nonNull(banner)) {
            banner.setStatus(0);
            redisTemplate.opsForValue().set(bannerKey, JsonUtil.toJson(banner));
        }
    }

    /**
     * 保存Banner详情和日期索引
     * Banner详情：bannerInfo:bannerId:{bannerId}
     * 日期索引：banners:productId:{productId}:date:{date}（为有效期内每一天创建索引）
     */
    private void saveBannerWithIndex(Banner banner) {
        String bannerKey = String.format("bannerInfo:bannerId:%d", banner.getId());
        redisTemplate.opsForValue().set(bannerKey, JsonUtil.toJson(banner));

        // 根据startTime和endTime计算日期范围，为每一天创建索引
        List<String> dateRange = DateUtil.getDateRange(banner.getStartTime(), banner.getEndTime());
        for (String date : dateRange) {
            String productDateKey = String.format("banners:productId:%d:date:%s", banner.getProductId(), date);
            redisTemplate.opsForSet().add(productDateKey, String.valueOf(banner.getId()));
        }
    }

    /**
     * 获取Banner详情（带两级缓存）
     * 先查Caffeine本地缓存，未命中再查Redis，更新本地缓存
     */
    private Banner getBanner(Long bannerId) {
        String bannerKey = String.format("bannerInfo:bannerId:%d", bannerId);

        // 一级缓存：Caffeine本地缓存（30秒TTL）
        Banner cached = bannerLocalCache.getIfPresent(bannerKey);
        if (Objects.nonNull(cached)) {
            return cached;
        }

        // 二级缓存：Redis
        Object value = redisTemplate.opsForValue().get(bannerKey);
        if (Objects.isNull(value)) {
            return null;
        }

        Banner banner = JsonUtil.fromJson(value.toString(), Banner.class);
        bannerLocalCache.put(bannerKey, banner);
        return banner;
    }

    /**
     * 获取产品+日期对应的活跃Banner列表
     * 1. 先查本地缓存获取Banner ID列表
     * 2. 根据ID逐个查Banner详情（带本地缓存）
     * 3. 过滤status=1的活跃Banner，按优先级降序排列
     */
    private List<Banner> getActiveBanners(Integer productId, String date) {
        String productDateKey = String.format("banners:productId:%d:date:%s", productId, date);

        // 先查本地缓存获取Banner ID列表
        Set<Object> bannerIds = productDateLocalCache.getIfPresent(productDateKey);
        if (Objects.isNull(bannerIds)) {
            bannerIds = redisTemplate.opsForSet().members(productDateKey);
            if (Objects.nonNull(bannerIds) && !bannerIds.isEmpty()) {
                productDateLocalCache.put(productDateKey, bannerIds);
            }
        }

        if (Objects.isNull(bannerIds) || bannerIds.isEmpty()) {
            return new ArrayList<>();
        }

        // 根据ID逐个查详情，过滤活跃Banner，按优先级降序排列
        return bannerIds.stream()
                .map(obj -> {
                    Long id = Long.parseLong(obj.toString());
                    Banner banner = getBanner(id);
                    return (Objects.nonNull(banner) && banner.isActive()) ? banner : null;
                })
                .filter(Objects::nonNull)
                .sorted((a, b) -> {
                    if (Objects.isNull(a.getPriority())) return 1;
                    if (Objects.isNull(b.getPriority())) return -1;
                    return b.getPriority().compareTo(a.getPriority());
                })
                .collect(Collectors.toList());
    }

    /**
     * 检查用户是否在Banner的人群包中
     * 分桶策略：userId % bucketCount → 定位到具体桶，检查用户是否在桶中
     */
    private boolean isUserInBucket(Long bannerId, Long userId) {
        Banner banner = getBanner(bannerId);
        if (Objects.isNull(banner)) {
            return false;
        }

        int bucketIndex = (int) (Math.abs(userId) % banner.getBucketCount());
        String bucketKey = String.format("bannerUsers:bannerId:%d:bucketIndex:%d", bannerId, bucketIndex);
        Boolean isMember = redisTemplate.opsForSet().isMember(bucketKey, String.valueOf(userId));
        return Boolean.TRUE.equals(isMember);
    }

    /**
     * 添加用户到人群包桶中
     * 按userId % bucketCount分配桶索引，批量写入Redis
     */
    private void addUserToBuckets(Long bannerId, int bucketCount, List<Long> userIds) {
        Map<Integer, List<String>> bucketMap = new HashMap<>();
        for (Long userId : userIds) {
            int bucketIndex = (int) (Math.abs(userId) % bucketCount);
            bucketMap.computeIfAbsent(bucketIndex, k -> new ArrayList<>()).add(String.valueOf(userId));
        }

        for (Map.Entry<Integer, List<String>> entry : bucketMap.entrySet()) {
            String bucketKey = String.format("bannerUsers:bannerId:%d:bucketIndex:%d", bannerId, entry.getKey());
            redisTemplate.opsForSet().add(bucketKey, entry.getValue().toArray());
        }
    }

    /**
     * 删除所有用户桶数据
     * 遍历所有桶索引，逐个删除Redis Key
     */
    private void deleteAllUserBuckets(Long bannerId, int bucketCount) {
        for (int i = 0; i < bucketCount; i++) {
            String bucketKey = String.format("bannerUsers:bannerId:%d:bucketIndex:%d", bannerId, i);
            redisTemplate.delete(bucketKey);
        }
    }
}
