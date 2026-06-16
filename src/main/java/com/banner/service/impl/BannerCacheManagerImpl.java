package com.banner.service.impl;

import com.banner.common.constant.BannerConstants;
import com.banner.common.dto.BannerInfo;
import com.banner.common.util.JsonUtil;
import com.banner.common.util.XssUtil;
import com.banner.service.BannerCacheManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Banner缓存管理实现
 * <p>
 * 使用Redis Hash存储Banner数据，Key格式：banner:{productId}:{date}
 * Hash的field为bannerId，value为BannerInfo的JSON字符串。
 * </p>
 */
@Service
public class BannerCacheManagerImpl implements BannerCacheManager {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    /**
     * 设置单个Banner到缓存
     * <p>
     * 处理流程：
     * 1. 构建缓存Key：banner:{productId}:{date}
     * 2. 查询已有数据进行版本号校验（防止旧数据覆盖新数据）
     * 3. 对Banner内容进行XSS过滤（HTML转义、URL协议校验）
     * 4. 写入Redis Hash
     * </p>
     */
    @Override
    public void setBanner(String productId, String date, BannerInfo bannerInfo) {
        String cacheKey = BannerConstants.BANNER_CACHE_KEY_PREFIX + productId + ":" + date;

        String existingJson = (String) redisTemplate.opsForHash().get(cacheKey, bannerInfo.getBannerId());
        if (existingJson != null) {
            BannerInfo existing = JsonUtil.fromJson(existingJson, BannerInfo.class);
            if (existing.getVersion() != null && existing.getVersion() >= bannerInfo.getVersion()) {
                return;
            }
        }

        sanitizeBannerInfo(bannerInfo);
        redisTemplate.opsForHash().put(cacheKey, bannerInfo.getBannerId(), JsonUtil.toJson(bannerInfo));
    }

    /**
     * XSS内容过滤
     * <p>
     * 对Banner的标题、图片URL、链接URL进行安全处理：
     * 1. 标题：HTML转义
     * 2. 图片URL：协议白名单校验（仅允许http/https）
     * 3. 链接URL：协议白名单校验 + 禁止javascript协议
     * </p>
     */
    private void sanitizeBannerInfo(BannerInfo bannerInfo) {
        if (bannerInfo == null) {
            return;
        }
        bannerInfo.setTitle(XssUtil.escapeHtml(bannerInfo.getTitle()));
        bannerInfo.setImageUrl(XssUtil.sanitizeUrl(bannerInfo.getImageUrl()));
        bannerInfo.setLinkUrl(XssUtil.sanitizeLinkUrl(bannerInfo.getLinkUrl()));
    }

    /**
     * 从缓存删除单个Banner
     * <p>
     * 从Redis Hash中删除指定bannerId的字段。
     * </p>
     */
    @Override
    public void deleteBanner(String productId, String date, String bannerId) {
        String cacheKey = BannerConstants.BANNER_CACHE_KEY_PREFIX + productId + ":" + date;
        redisTemplate.opsForHash().delete(cacheKey, bannerId);
    }

    /**
     * 从缓存获取指定日期的Banner列表
     * <p>
     * 查询流程：
     * 1. 获取Redis Hash的所有字段
     * 2. 反序列化为BannerInfo对象
     * 3. 过滤状态为启用（status=1）的Banner
     * 4. 按优先级降序排序
     * </p>
     */
    @Override
    public List<BannerInfo> getBannersByDate(String productId, String date) {
        String cacheKey = BannerConstants.BANNER_CACHE_KEY_PREFIX + productId + ":" + date;

        Map<Object, Object> entries = redisTemplate.opsForHash().entries(cacheKey);
        if (entries == null || entries.isEmpty()) {
            return new ArrayList<>();
        }

        return entries.values().stream()
                .map(v -> JsonUtil.fromJson((String) v, BannerInfo.class))
                .filter(b -> b.getStatus() == 1)
                .sorted((a, b) -> b.getPriority().compareTo(a.getPriority()))
                .collect(Collectors.toList());
    }

    /**
     * 全量刷新指定日期的Banner缓存
     * <p>
     * 用于定时任务兜底：
     * 1. 删除旧缓存Key
     * 2. 遍历Banner列表，只写入状态为启用的Banner
     * </p>
     */
    @Override
    public void refreshBannersByDate(String productId, String date, List<BannerInfo> banners) {
        String cacheKey = BannerConstants.BANNER_CACHE_KEY_PREFIX + productId + ":" + date;

        redisTemplate.delete(cacheKey);

        for (BannerInfo banner : banners) {
            if (banner.getStatus() == 1) {
                redisTemplate.opsForHash().put(cacheKey, banner.getBannerId(), JsonUtil.toJson(banner));
            }
        }
    }
}
