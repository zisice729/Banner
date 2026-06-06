package com.banner.mq.consumer;

import com.banner.client.BannerOperationClient;
import com.banner.common.constant.BannerConstants;
import com.banner.common.dto.mq.BannerMessage;
import com.banner.common.dto.request.BannerSyncRequest;
import com.banner.common.enums.MessageType;
import com.banner.common.util.JsonUtil;
import com.banner.common.util.RedisKeyBuilder;
import com.banner.lock.RedisDistributedLock;
import com.banner.repository.MqConsumeRecordRepository;
import com.banner.service.BannerCacheManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Component
public class BannerSyncConsumer {

    private static final Logger log = LoggerFactory.getLogger(BannerSyncConsumer.class);

    @Autowired
    private BannerOperationClient bannerOperationClient;

    @Autowired
    private BannerCacheManager bannerCacheManager;

    @Autowired
    private MqConsumeRecordRepository mqConsumeRecordRepository;

    @Autowired
    private RedisDistributedLock redisDistributedLock;

    @KafkaListener(topics = BannerConstants.KAFKA_TOPIC_BANNER_SYNC, groupId = BannerConstants.KAFKA_CONSUMER_GROUP_ID)
    public void consume(String message) {
        BannerMessage bannerMessage = JsonUtil.fromJson(message, BannerMessage.class);
        Long id = bannerMessage.getId();

        // 幂等：用messageId做唯一键，主键冲突则说明已消费过
        try {
            mqConsumeRecordRepository.insert(bannerMessage.getMessageId(), id);
        } catch (DuplicateKeyException e) {
            log.warn("duplicate consume, messageId={}, id={}", bannerMessage.getMessageId(), id);
            return;
        }

        if (Objects.equals(bannerMessage.getType(), MessageType.DELETE.getCode())) {
            bannerCacheManager.deleteBannerCache(id);
            return;
        }

        // 加分布式锁，保证同一banner的缓存更新串行执行
        String lockKey = RedisKeyBuilder.bannerLock(id);
        String lockValue = redisDistributedLock.tryLockWithRetry(lockKey);
        if (Objects.isNull(lockValue)) {
            log.warn("tryLock failed, id={}", id);
            return;
        }

        try {
            // 1. RPC调用运营端获取Banner基本信息
            BannerSyncRequest data = bannerOperationClient.getBannerById(id);
            if (Objects.isNull(data)) {
                log.error("rpc getBannerById return null, id={}", id);
                return;
            }

            // 2. 分页RPC获取用户列表
            int bucketSize = BannerConstants.USER_BUCKET_SIZE;
            int page = 0;
            List<Long> allUserIds = new ArrayList<>();

            while (true) {
                List<Long> pageUserIds = bannerOperationClient.getUserIdsByPage(id, page, bucketSize);
                if (Objects.isNull(pageUserIds) || pageUserIds.isEmpty()) {
                    break;
                }
                allUserIds.addAll(pageUserIds);
                page++;
            }

            data.setUserIds(allUserIds);

            // 3. 更新缓存（分桶写入Redis + 更新Banner信息 + 发布本地缓存失效通知）
            bannerCacheManager.refreshBannerCache(id, data);

        } catch (Exception e) {
            log.error("sync banner cache failed, id={}", id, e);
        } finally {
            redisDistributedLock.unlock(lockKey, lockValue);
        }
    }
}
