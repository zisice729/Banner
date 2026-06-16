package com.banner.service.impl;

import com.banner.common.dto.BannerInfo;
import com.banner.common.entity.Banner;
import com.banner.common.util.DateUtil;
import com.banner.common.util.StringUtils;
import com.banner.convert.BannerConvert;
import com.banner.lock.RedisDistributedLock;
import com.banner.repository.BannerRepository;
import com.banner.service.BannerCacheManager;
import com.banner.service.BannerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Banner业务逻辑实现
 * <p>
 * 核心职责：
 * 1. 查询Banner列表（从Redis缓存）
 * 2. 从Kafka同步Banner数据到Redis缓存
 * 3. 从MySQL数据库刷新Banner数据到Redis缓存
 * </p>
 */
@Service
public class BannerServiceImpl implements BannerService {

    @Autowired
    private BannerRepository bannerRepository;

    @Autowired
    private BannerCacheManager bannerCacheManager;

    @Autowired
    private RedisDistributedLock redisDistributedLock;

    @Autowired
    private BannerConvert bannerConvert;

    /**
     * 获取指定日期的Banner列表
     * <p>
     * 从Redis缓存中查询，日期为空时默认使用当天日期。
     * 缓存Key格式：banner:{productId}:{date}
     * </p>
     */
    @Override
    public List<BannerInfo> getBannersByDate(String productId, String date) {
        if (StringUtils.isEmpty(date)) {
            date = DateUtil.todayStr();
        }
        return bannerCacheManager.getBannersByDate(productId, date);
    }

    /**
     * 从Kafka同步Banner数据
     * <p>
     * 处理流程：
     * 1. 计算Banner生效日期范围（startDay到endDay）
     * 2. 遍历每个日期，获取分布式锁（防止多实例并发冲突）
     * 3. 根据changeType执行操作：DELETE删除缓存，UPDATE写入缓存
     * 4. 释放分布式锁
     * </p>
     */
    @Override
    @Transactional
    public void syncBannerFromKafka(BannerInfo bannerInfo) {
        List<String> dates = DateUtil.getDateRange(bannerInfo.getStartDay(), bannerInfo.getEndDay());

        for (String date : dates) {
            String lockValue = redisDistributedLock.tryLockWithRetry(bannerInfo.getProductId(), date);
            if (lockValue == null) {
                throw new RuntimeException("Failed to acquire lock for banner sync: " + bannerInfo.getBannerId());
            }

            try {
                if ("DELETE".equals(bannerInfo.getChangeType())) {
                    bannerCacheManager.deleteBanner(bannerInfo.getProductId(), date, bannerInfo.getBannerId());
                } else {
                    bannerCacheManager.setBanner(bannerInfo.getProductId(), date, bannerInfo);
                }
            } finally {
                redisDistributedLock.unlock(bannerInfo.getProductId(), date);
            }
        }
    }

    /**
     * 从数据库刷新Banner数据
     * <p>
     * 用于定时任务兜底，保证数据最终一致性：
     * 1. 从MySQL查询指定日期的Banner数据
     * 2. 转换为BannerInfo对象
     * 3. 全量刷新到Redis缓存
     * </p>
     */
    @Override
    @Transactional
    public void refreshFromDatabase(String productId, String date) {
        LocalDate localDate = DateUtil.parseYYYYMMDD(date);
        List<Banner> banners = bannerRepository.findByProductIdAndDate(productId, localDate);

        List<BannerInfo> bannerInfos = banners.stream()
                .map(bannerConvert::toBannerInfo)
                .collect(Collectors.toList());

        bannerCacheManager.refreshBannersByDate(productId, date, bannerInfos);
    }
}
