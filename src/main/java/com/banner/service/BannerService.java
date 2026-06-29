package com.banner.service;

import com.banner.client.BannerOperationClient;
import com.banner.common.constant.BannerConstants;
import com.banner.common.dto.Banner;
import com.banner.common.dto.BannerQueryResponse;
import com.banner.common.util.DateUtil;
import com.banner.common.util.JsonUtil;
import com.google.common.cache.LoadingCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Banner服务层
 * 核心职责：Banner查询、数据同步、人群包分桶管理
 * 数据存储：Redis + Guava本地缓存（5分钟TTL自动失效）
 */
@Service
public class BannerService {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    @Autowired
    private LoadingCache<String, Banner> bannerLocalCache;

    @Autowired
    private LoadingCache<String, Set<Object>> productDateLocalCache;

    @Autowired
    private BannerOperationClient bannerOperationClient;

    /**
     * 查询Banner列表
     * 根据产品ID和用户ID筛选出用户可见的Banner，日期由系统内部获取（当前日期）
     * 直接在方法内完成所有逻辑，避免方法调用链路过长
     *
     * @param productId 产品ID
     * @param userId    用户ID
     * @return BannerQueryResponse 包含产品ID、日期和Banner列表
     */
    public BannerQueryResponse queryBanners(Integer productId, Long userId) {
        // 获取当前日期
        String date = DateUtil.getCurrentDate();

        // 构建产品+日期索引Key，获取该日期下的bannerId集合
        String productDateKey = String.format("banners:productId:%d:date:%s", productId, date);
        Set<Object> bannerIds;
        try {
            bannerIds = productDateLocalCache.get(productDateKey);
        } catch (Exception e) {
            bannerIds = null;
        }

        if (Objects.isNull(bannerIds) || bannerIds.isEmpty()) {
            return new BannerQueryResponse(productId, date, new ArrayList<>());
        }

        // 根据bannerId获取详情，过滤活跃状态，检查用户是否在人群包中，按优先级排序
        List<Banner> matchedBanners = bannerIds.stream()
                .map(obj -> {
                    Long id = Long.parseLong(obj.toString());
                    Banner banner = getBanner(id);
                    if (Objects.isNull(banner) || !banner.isActive()) {
                        return null;
                    }

                    // 检查用户是否在人群包中（分桶策略：userId % bucketCount）
                    int bucketIndex = (int) (Math.abs(userId) % banner.getBucketCount());
                    String bucketKey = String.format("bannerUsers:bannerId:%d:bucketIndex:%d", id, bucketIndex);
                    Boolean isMember = redisTemplate.opsForSet().isMember(bucketKey, String.valueOf(userId));
                    return Boolean.TRUE.equals(isMember) ? banner : null;
                })
                .filter(Objects::nonNull)
                .sorted((a, b) -> {
                    if (Objects.isNull(a.getPriority())) return 1;
                    if (Objects.isNull(b.getPriority())) return -1;
                    return b.getPriority().compareTo(a.getPriority());
                })
                .collect(Collectors.toList());

        // 转换为响应DTO
        List<BannerQueryResponse.BannerSimpleInfo> simpleInfos = matchedBanners.stream()
                .map(b -> new BannerQueryResponse.BannerSimpleInfo(
                        b.getId(), b.getTitle(), b.getImageUrl(), b.getLinkUrl(), b.getPriority()))
                .collect(Collectors.toCollection(ArrayList::new));

        return new BannerQueryResponse(productId, date, simpleInfos);
    }

    /**
     * 同步Banner数据
     * 直接在方法内完成所有逻辑：获取Banner信息 → 获取全部用户 → 计算桶数 → 删除旧数据 → 写入新数据 → 保存索引
     * 依靠本地缓存5分钟TTL自动过期，无需手动invalidate
     *
     * @param bannerId Banner ID
     */
    public void syncBanner(Long bannerId) {
        // Step1: RPC获取Banner基本信息
        Banner banner = bannerOperationClient.getBannerById(bannerId);
        if (Objects.isNull(banner)) {
            throw new RuntimeException("RPC getBannerById return null, bannerId=" + bannerId);
        }

        // Step2: 分页RPC获取全部用户列表（先收集到内存，再统一处理）
        List<Long> allUserIds = new ArrayList<>();
        int page = 0;
        int maxPage = BannerConstants.MAX_RPC_PAGE_COUNT;
        while (page < maxPage) {
            List<Long> pageUserIds = bannerOperationClient.getUserIdsByPage(bannerId, page, BannerConstants.USER_BUCKET_SIZE);
            if (Objects.isNull(pageUserIds) || pageUserIds.isEmpty()) {
                break;
            }
            allUserIds.addAll(pageUserIds);
            page++;
        }

        if (page >= maxPage) {
            throw new RuntimeException("Exceed max page limit, bannerId=" + bannerId);
        }

        // Step3: 根据用户总数计算桶数量（公式：ceil(用户数/桶大小)，最大不超过MAX_BUCKET_COUNT）
        int bucketCount = allUserIds.size() <= BannerConstants.USER_BUCKET_SIZE
                ? 1
                : Math.min((int) Math.ceil((double) allUserIds.size() / BannerConstants.USER_BUCKET_SIZE), BannerConstants.MAX_BUCKET_COUNT);
        banner.setBucketCount(bucketCount);

        // Step4: 删除旧的用户桶数据
        for (int i = 0; i < bucketCount; i++) {
            String bucketKey = String.format("bannerUsers:bannerId:%d:bucketIndex:%d", bannerId, i);
            redisTemplate.delete(bucketKey);
        }

        // Step5: 将用户按userId取模分配到桶中，批量写入Redis Set（25小时过期）
        Map<Integer, List<String>> bucketMap = new HashMap<>();
        for (Long userId : allUserIds) {
            int bucketIndex = (int) (Math.abs(userId) % bucketCount);
            bucketMap.computeIfAbsent(bucketIndex, k -> new ArrayList<>()).add(String.valueOf(userId));
        }
        for (Map.Entry<Integer, List<String>> entry : bucketMap.entrySet()) {
            String bucketKey = String.format("bannerUsers:bannerId:%d:bucketIndex:%d", bannerId, entry.getKey());
            redisTemplate.opsForSet().add(bucketKey, entry.getValue().toArray());
            redisTemplate.expire(bucketKey, 25 * 60 * 60, TimeUnit.SECONDS);
        }

        // Step6: 保存Banner详情到Redis（25小时过期）
        String bannerKey = String.format("bannerInfo:bannerId:%d", bannerId);
        redisTemplate.opsForValue().set(bannerKey, JsonUtil.toJson(banner), 25 * 60 * 60, TimeUnit.SECONDS);

        // Step7: 保存日期索引到Redis（为有效期内每一天创建索引，25小时过期）
        List<String> dateRange = DateUtil.getDateRange(banner.getStartTime(), banner.getEndTime());
        for (String date : dateRange) {
            String productDateKey = String.format("banners:productId:%d:date:%s", banner.getProductId(), date);
            redisTemplate.opsForSet().add(productDateKey, String.valueOf(bannerId));
            redisTemplate.expire(productDateKey, 25 * 60 * 60, TimeUnit.SECONDS);
        }
    }

    /**
     * 设置Banner为非活跃状态（软删除）
     * 不删除人群包数据，只设置status=0，查询时会过滤
     * 依靠本地缓存5分钟TTL自动过期，无需手动invalidate
     *
     * @param bannerId Banner ID
     */
    public void setBannerInactive(Long bannerId) {
        String bannerKey = String.format("bannerInfo:bannerId:%d", bannerId);
        Banner banner = getBanner(bannerId);
        if (Objects.nonNull(banner)) {
            banner.setStatus(0);
            redisTemplate.opsForValue().set(bannerKey, JsonUtil.toJson(banner), 25 * 60 * 60, TimeUnit.SECONDS);
        }
    }

    /**
     * 获取Banner详情（带两级缓存）
     * 使用Guava LoadingCache，本地缓存未命中时自动调用CacheLoader从Redis加载
     *
     * @param bannerId Banner ID
     * @return Banner对象，缓存未命中或加载失败返回null
     */
    private Banner getBanner(Long bannerId) {
        String bannerKey = String.format("bannerInfo:bannerId:%d", bannerId);
        try {
            return bannerLocalCache.get(bannerKey);
        } catch (Exception e) {
            return null;
        }
    }
}