# Banner消费端系统 - 设计文档

## 一、项目概述

Banner消费端系统是一个**高性能的Banner缓存同步与查询服务**，负责：
- 接收运营端通过Kafka推送的Banner变更通知，RPC调用运营端获取完整数据并同步到Redis
- 支持人群包功能，通过分桶策略解决BigKey问题
- 通过XXL-Job定时任务进行增量同步和全量一致性检查
- 为C端提供高性能的Banner查询接口，支持人群包过滤
- 本地缓存（Caffeine）+ Redis二级缓存，Redis Pub/Sub保证多机本地缓存一致性

### 核心特点

| 特性 | 说明 |
|------|------|
| **高性能读** | Caffeine本地缓存 + Redis二级缓存，毫秒级响应 |
| **人群包支持** | 分桶存储用户ID列表，解决BigKey问题 |
| **消息幂等** | messageId唯一索引 + DuplicateKeyException |
| **缓存一致性** | Redis Pub/Sub广播失效本地缓存 |
| **分布式锁** | 保证同一Banner的缓存更新串行执行 |
| **代码规范** | Objects.isNull/equals判空判等，String.format管理Redis Key |

---

## 二、架构设计

### 2.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────┐
│                       运营端（B端）                              │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  写入 simple_banner表 + banner_user_list表               │  │
│  │  发送MQ消息(id + type)                                    │  │
│  └─────────────────────────┬─────────────────────────────────┘  │
└────────────────────────────┼────────────────────────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ banner-sync-topic│  (单一Topic，type区分消息类型)
                    └────────┬────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    BannerSyncConsumer                           │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  1. 幂等检查(messageId唯一索引)                            │  │
│  │  2. 加分布式锁                                             │  │
│  │  3. RPC调用运营端获取Banner基本信息                         │  │
│  │  4. 分页RPC获取用户列表                                    │  │
│  │  5. 分桶写入Redis + 更新Banner信息缓存                     │  │
│  │  6. 发布本地缓存失效通知(Pub/Sub)                          │  │
│  └───────────────────────────────────────────────────────────┘  │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                 ┌──────────────┼──────────────┐
                 ▼              ▼              ▼
          ┌───────────┐  ┌───────────┐  ┌───────────┐
          │  Redis    │  │ Caffeine  │  │  XXL-Job  │
          │ (主缓存)  │  │ (本地缓存)│  │ (定时任务)│
          └───────────┘  └───────────┘  └───────────┘
                 │              ▲
                 │  Pub/Sub     │
                 └──────────────┘
```

### 2.2 核心模块职责

| 模块 | 职责 | 关键组件 |
|------|------|----------|
| **common** | 公共模块：常量、枚举、DTO、工具类 | `BannerConstants`, `MessageType`, `RedisKeyBuilder` |
| **client** | RPC客户端：调用运营端获取数据 | `BannerOperationClient` |
| **mq** | 消息队列：单一Topic生产者+消费者 | `BannerMqProducer`, `BannerSyncConsumer` |
| **service** | 业务逻辑：缓存管理、查询服务 | `BannerCacheManager`, `BannerQueryService` |
| **controller** | 控制层：C端查询接口 | `BannerController` |
| **cache** | 缓存组件：本地缓存失效监听 | `CacheInvalidationListener` |
| **config** | 配置类：Redis、Kafka、Caffeine、数据源 | `RedisConfig`, `GuavaCacheConfig` |
| **scheduler** | 定时任务：增量同步+全量一致性检查 | `BannerScheduledTask` |
| **lock** | 分布式锁 | `RedisDistributedLock` |
| **repository** | 数据访问：MQ消费记录（H2内存数据库） | `MqConsumeRecordRepository` |

---

## 三、项目文件目录

```
src/main/java/com/banner/
├── BannerApplication.java                     # 启动类
│
├── cache/                                     # 缓存组件
│   └── CacheInvalidationListener.java         # Redis Pub/Sub本地缓存失效监听
│
├── client/                                    # RPC客户端
│   └── BannerOperationClient.java             # 调用运营端获取Banner数据（含分页获取用户列表）
│
├── common/                                    # 公共模块
│   ├── constant/
│   │   └── BannerConstants.java               # 常量（Topic、锁、分桶配置）
│   ├── dto/
│   │   ├── ApiResponse.java                   # 通用API响应
│   │   ├── mq/
│   │   │   └── BannerMessage.java             # MQ消息体（messageId + id + type）
│   │   ├── request/
│   │   │   ├── BannerQueryRequest.java        # 查询请求
│   │   │   └── BannerSyncRequest.java         # RPC返回的完整Banner数据
│   │   └── response/
│   │       └── BannerQueryResponse.java       # 查询响应
│   ├── enums/
│   │   └── MessageType.java                   # 消息类型枚举（SYNC=1, DELETE=2）
│   └── util/
│       ├── DateUtil.java                      # 日期工具
│       ├── JsonUtil.java                      # JSON工具
│       ├── RedisKeyBuilder.java               # Redis Key构建器（String.format）
│       └── StringUtils.java                   # 字符串工具
│
├── config/                                    # 配置类
│   ├── DataSourceConfig.java                  # H2数据源配置（MQ消费记录表）
│   ├── CaffeineCacheConfig.java                # Caffeine本地缓存配置
│   ├── KafkaConfig.java                       # Kafka配置
│   └── RedisConfig.java                       # Redis配置 + Pub/Sub监听器
│
├── controller/                                # 控制层
│   └── BannerController.java                  # C端查询接口
│
├── lock/                                      # 分布式锁
│   └── RedisDistributedLock.java              # Redis分布式锁（带重试+Lua释放）
│
├── mq/                                        # 消息队列
│   ├── consumer/
│   │   └── BannerSyncConsumer.java            # 统一消费者（幂等+分布式锁+RPC+分桶）
│   └── producer/
│       └── BannerMqProducer.java              # 生产者（发送id+type+messageId）
│
├── repository/                                # 数据访问层
│   └── MqConsumeRecordRepository.java         # MQ消费记录（H2内存数据库）
│
├── scheduler/                                 # 定时任务
│   └── BannerScheduledTask.java               # @XxlJob增量同步+全量一致性检查
│
└── service/                                   # 服务层
    ├── BannerCacheManager.java                # 缓存管理接口
    ├── BannerQueryService.java                # 查询服务接口
    └── impl/
        ├── BannerCacheManagerImpl.java        # 缓存管理实现（Redis分桶+Caffeine+Pub/Sub）
        └── BannerQueryServiceImpl.java        # 查询服务实现
```

---

## 四、核心流程

### 4.1 MQ消息流程

```
运营端写DB完成 → BannerMqProducer.sendBannerSync(id)
    → Kafka(banner-sync-topic, {messageId, id, type=1})
    → BannerSyncConsumer.consume()
        → 幂等检查(INSERT mq_consume_record, DuplicateKeyException)
        → 加分布式锁(banner:{id}:lock)
        → RPC: BannerOperationClient.getBannerById(id) → Banner基本信息
        → RPC: BannerOperationClient.getUserIdsByPage(id, page, size) → 分页获取用户列表
        → BannerCacheManager.refreshBannerCache(id, data)
            → Redis SET banner:{id}:info
            → Redis SADD banner:{id}:users:{bucketIndex} (分桶)
            → Redis SET banner:{id}:users:bucket_count
            → Redis SADD banner:product:{productId}:date:{date}
            → Redis PUBLISH banner:cache:invalidate {id}
        → 释放锁
```

### 4.2 查询流程

```
C端请求 → BannerController.getBanners(BannerQueryRequest)
    → BannerQueryService.getBannersByProductDateAndUserId(productId, date, userId)
        → BannerCacheManager.getBannersByProductAndDate(productId, date)
            → Caffeine本地缓存 → Redis → 返回Banner列表
        → BannerCacheManager.containsUserId(id, userId)
            → Redis SISMEMBER banner:{id}:users:{bucketIndex} userId
        → 过滤后返回
```

### 4.3 本地缓存一致性

```
机器A更新缓存 → Redis PUBLISH banner:cache:invalidate {id}
    → 所有机器CacheInvalidationListener收到消息
    → 清除Caffeine本地缓存中对应的key
    → 下次查询时从Redis重新加载
```

---

## 五、MQ消息设计

### 5.1 消息体

```java
public class BannerMessage {
    private String messageId;    // UUID，幂等标识
    private Long id;             // Banner的数据库ID
    private Integer type;        // 消息类型：1=同步, 2=删除
}
```

### 5.2 消息类型

| 类型 | Code | 说明 |
|------|------|------|
| SYNC | 1 | Banner新增/更新，消费者RPC获取完整数据后更新缓存 |
| DELETE | 2 | Banner删除，消费者直接删除缓存 |

### 5.3 幂等机制

```
消费者收到消息 → INSERT mq_consume_record(message_id, banner_id)
    → 成功：继续处理
    → DuplicateKeyException：说明已消费过，跳过
```

- `mq_consume_record`表使用H2内存数据库存储
- `message_id`为主键，唯一索引保证幂等
- 每条消息的messageId由生产者生成（UUID）

---

## 六、分桶策略

### 6.1 解决的问题

人群包可能包含数十万甚至上百万用户ID，直接存为一个Redis Set会导致BigKey，阻塞Redis。

### 6.2 分桶方案

| 配置 | 值 | 说明 |
|------|-----|------|
| USER_BUCKET_SIZE | 5000 | 每个桶最多存储5000个用户ID |

**Redis Key设计**：

| Key | 类型 | 说明 |
|-----|------|------|
| `banner:{id}:users:{bucketIndex}` | Set | 分桶存储用户ID |
| `banner:{id}:users:bucket_count` | String | 桶数量 |

**写入流程**：
```
分页RPC获取用户列表(每页5000)
    → 每页写入一个Redis Set桶
    → 记录桶数量
```

**查询流程**：
```
containsUserId(id, userId):
    → 获取桶数量
    → 遍历所有桶执行 SISMEMBER
    → 找到即返回true

getUserIdsFromCache(id):
    → 获取桶数量
    → 遍历所有桶执行 SMEMBERS
    → 合并返回
```

---

## 七、Redis Key设计

所有Key使用`String.format`统一管理：

| Key模板 | 示例 | 类型 | 说明 |
|---------|------|------|------|
| `banner:%d:info` | `banner:123:info` | String | Banner基本信息JSON |
| `banner:%d:users:%d` | `banner:123:users:0` | Set | 用户ID分桶 |
| `banner:%d:users:bucket_count` | `banner:123:users:bucket_count` | String | 桶数量 |
| `banner:%d:lock` | `banner:123:lock` | String | 分布式锁 |
| `banner:product:%d:date:%s` | `banner:product:1:date:20260607` | Set | 按产品+日期索引Banner ID |

**Pub/Sub频道**：`banner:cache:invalidate`，消息体为bannerId

---

## 八、数据库表设计（运营端）

### 8.1 simple_banner 表

| 字段名 | 类型 | 约束 | 说明 |
|--------|------|------|------|
| id | BIGINT | PRIMARY KEY, AUTO_INCREMENT | 自增主键（即bannerId） |
| product_id | INT | NOT NULL | 业务线标识 |
| title | VARCHAR(128) | NOT NULL | 标题 |
| image_url | VARCHAR(512) | NOT NULL | 图片地址 |
| link_url | VARCHAR(512) | NULL | 跳转链接 |
| priority | INT | NOT NULL, DEFAULT 0 | 优先级 |
| status | INT | NOT NULL, DEFAULT 1 | 状态：0-禁用，1-启用 |
| start_time | BIGINT | NOT NULL | 生效开始时间（时间戳） |
| end_time | BIGINT | NOT NULL | 生效结束时间（时间戳） |
| create_time | BIGINT | NOT NULL | 创建时间（时间戳） |
| update_time | BIGINT | NOT NULL | 更新时间（时间戳） |

### 8.2 banner_user_list 表

| 字段名 | 类型 | 约束 | 说明 |
|--------|------|------|------|
| id | BIGINT | PRIMARY KEY, AUTO_INCREMENT | 自增主键 |
| banner_id | BIGINT | NOT NULL | 关联simple_banner.id |
| user_id | BIGINT | NOT NULL | 用户ID |
| create_time | BIGINT | NOT NULL | 创建时间（时间戳） |

**索引**：`UNIQUE KEY uk_banner_user (banner_id, user_id)`

### 8.3 mq_consume_record 表（本服务H2内存数据库）

| 字段名 | 类型 | 约束 | 说明 |
|--------|------|------|------|
| message_id | VARCHAR(64) | PRIMARY KEY | 消息唯一ID |
| banner_id | BIGINT | NOT NULL | Banner ID |
| create_time | BIGINT | NOT NULL | 创建时间 |

---

## 九、并发控制

### 9.1 分布式锁

消费者处理同步消息时，对同一Banner加分布式锁，保证缓存更新串行执行：

```
lockKey = banner:{id}:lock
tryLockWithRetry(lockKey)  → 指数退避重试（100ms → 300ms → 900ms）
unlock(lockKey, lockValue) → Lua脚本原子释放
```

### 9.2 缓存更新原子性

在分布式锁保护下：
1. 先删除旧桶
2. 分页RPC获取用户列表并分桶写入
3. 更新Banner信息缓存
4. 发布本地缓存失效通知

---

## 十、定时任务

使用XXL-Job调度平台，`@XxlJob`注解：

| 任务 | Handler | 说明 |
|------|---------|------|
| 增量同步 | `incrementalSyncJob` | RPC获取增量更新的Banner ID列表，逐个刷新缓存 |
| 全量一致性检查 | `fullConsistencyCheckJob` | RPC获取所有活跃Banner ID，对比Redis缓存，修复不一致数据 |

---

## 十一、配置说明

| 配置项 | 说明 |
|--------|------|
| `spring.kafka.bootstrap-servers` | Kafka地址 |
| `spring.data.redis.host/port` | Redis地址 |
| `spring.datasource.url` | H2内存数据库（MQ消费记录） |
| `banner.operation.base-url` | 运营端RPC地址 |
| `xxl.job.admin.addresses` | XXL-Job调度中心地址 |
