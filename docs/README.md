# Banner消费端系统 - 设计文档

## 一、项目概述

Banner消费端系统是一个**高性能的Banner数据同步与查询服务**，负责：
- 接收管理系统通过Kafka推送的Banner数据并同步到Redis
- 支持人群包功能，实现用户级别的精准投放
- 通过定时任务连接MySQL进行数据兜底刷新，确保最终一致性
- 为C端提供高性能的Banner查询接口，支持人群包过滤

### 核心特点

| 特性 | 说明 |
|------|------|
| **高性能读** | C端直接读取Redis，毫秒级响应 |
| **人群包支持** | 通过分桶策略支持大规模用户ID列表存储 |
| **最终一致性** | 多级保障机制确保数据正确 |
| **消息顺序保证** | 单分区顺序+版本号机制 |
| **代码规范** | 分层清晰，Service层采用接口+实现类 |
| **模块化设计** | common模块统一管理公共组件 |

---

## 二、架构设计

### 2.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           后台管理系统                                       │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  写入 SimpleBanner表  │  写入 Banner表(多userId行)                    │  │
│  └────────────────────┬────────────────────────────────────────────────┘  │
│                       │                                                   │
│                       ▼                                                   │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  查询所有userId → 分批发送(每批1000) → 发送消息                        │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────┬─────────────────────────────────────────┘
                                  │
          ┌───────────────────────┼───────────────────────┬────────────────┐
          ▼                       ▼                       ▼                │
   ┌─────────────┐      ┌───────────────────┐      ┌─────────────┐         │
   │banner-topic │      │banner-userlist-   │      │ banner-     │         │
   │(UPDATE)     │      │batch-topic(UPDATE)│      │ delete-topic│         │
   └──────┬──────┘      └────────┬──────────┘      └──────┬──────┘         │
          │                       │                       │                 │
          ▼                       ▼                       ▼                 │
   ┌─────────────────────────────────────────────────────────────────────┐   │
   │                       BannerConsumer                                │   │
   │  ┌─────────────────────┐  ┌─────────────────────┐                    │   │
   │  │ BannerConsumer      │  │ (DELETE处理在同步服务) │                    │   │
   │  │ 处理 UPDATE          │  │                     │                    │   │
   │  └─────────────────────┘  └─────────────────────┘                    │   │
   │                       UserListConsumer                               │   │
   │  ┌─────────────────────────────────────────────────────────────────┐  │   │
   │  │ 处理批次消息 → 临时存储 → 全部到达后分桶 → 原子切换              │  │   │
   │  └─────────────────────────────────────────────────────────────────┘  │   │
   └───────────────────────────────┬───────────────────────────────────────┘   │
                                   │                                          │
                                   ▼                                          │
                          ┌───────────────┐      ┌─────────────┐             │
                          │     Redis     │      │   MySQL     │             │
                          │ ┌───────────┐ │      │SimpleBanner │             │
                          │ │主数据Hash │ │      │Banner       │             │
                          │ │人群包Set  │ │      └──────┬──────┘             │
                          │ └───────────┘ │             │                   │
                          └───────────────┘             └───────────────────┘
```

### 2.2 核心模块职责

| 模块 | 职责 | 关键组件 |
|------|------|----------|
| **common** | 公共模块：配置、工具类、常量、DTO、Entity | `BucketCalculator`, `RedisKeyBuilder`, `BannerConstants` |
| **controller** | 控制层：C端查询接口（支持userId过滤） | `BannersController` |
| **service** | 业务逻辑层：同步服务、查询服务、用户列表服务、缓存管理 | `BannerSyncService`, `BannerQueryService`, `BannerUserListService`, `UserBucketCacheManager` |
| **repository** | 数据访问层：SimpleBannerRepository、BannerUserRepository | `SimpleBannerRepository`, `BannerUserRepository` |
| **convert** | 转换器：Entity ↔ DTO | `BannerConvert` |
| **mq** | 消息队列：producer + consumer | `BannerMqProducer`, `BannerConsumer`, `UserListConsumer` |
| **scheduler** | 定时任务：增量刷新 + 全量一致性检查 | `BannerScheduledTask` |
| **lock** | 分布式锁：Redis分布式锁实现 | `RedisDistributedLock` |

---

## 三、项目文件目录

```
banner-consumer-system/
├── src/main/java/com/banner/
│   │
│   ├── BannerConsumerApplication.java       # 启动类
│   │
│   ├── common/                            # 公共模块
│   │   ├── config/                        # 配置类
│   │   │   ├── KafkaConfig.java          # Kafka配置（生产者/消费者）
│   │   │   └── RedisConfig.java         # Redis配置（模板+连接池）
│   │   ├── util/                          # 工具类
│   │   │   ├── DateUtil.java             # 日期工具（日期范围计算）
│   │   │   ├── JsonUtil.java             # JSON工具（序列化/反序列化）
│   │   │   ├── StringUtils.java          # 字符串工具
│   │   │   ├── BucketCalculator.java     # 分桶计算器（核心算法）
│   │   │   └── RedisKeyBuilder.java      # Redis Key构建器
│   │   ├── constant/                      # 常量定义
│   │   │   └── BannerConstants.java     # Banner常量（Topic、锁、分桶配置）
│   │   ├── dto/                          # 数据传输对象
│   │   │   ├── ApiResponse.java          # 统一响应封装
│   │   │   ├── SimpleBannerInfo.java     # 主消息DTO（Kafka传输）
│   │   │   ├── BannerInfo.java           # Banner信息DTO（API响应）
│   │   │   ├── BannerListResponse.java  # Banner列表响应DTO
│   │   │   └── UserListBatchMessage.java # 批次消息DTO
│   │   └── entity/                       # 数据库实体
│   │       ├── SimpleBanner.java         # 基础信息实体
│   │       └── Banner.java               # 人群包实体
│   │
│   ├── controller/                        # 控制层
│   │   └── BannersController.java        # C端查询接口
│   │
│   ├── service/                          # 业务逻辑层（接口）
│   │   ├── BannerSyncService.java        # Banner数据同步接口
│   │   ├── BannerQueryService.java       # Banner数据查询接口
│   │   ├── BannerUserListService.java    # 用户列表管理接口
│   │   ├── UserBucketCacheManager.java   # 用户桶缓存管理接口
│   │   ├── BannerCacheManager.java       # Banner缓存管理接口
│   │   └── impl/                        # 实现类
│   │       ├── BannerSyncServiceImpl.java   # 同步服务实现
│   │       ├── BannerQueryServiceImpl.java  # 查询服务实现
│   │       ├── BannerUserListServiceImpl.java # 用户列表服务实现
│   │       ├── UserBucketCacheManagerImpl.java # 用户桶缓存实现
│   │       └── BannerCacheManagerImpl.java    # Banner缓存实现
│   │
│   ├── repository/                      # 数据访问层
│   │   ├── SimpleBannerRepository.java # 基础信息数据访问
│   │   └── BannerUserRepository.java   # 人群包数据访问
│   │
│   ├── convert/                        # 转换器
│   │   └── BannerConvert.java         # Banner实体转换器
│   │
│   ├── mq/                            # 消息队列
│   │   ├── producer/                  # 生产者
│   │   │   └── BannerMqProducer.java # Banner消息生产者（批次发送）
│   │   └── consumer/                  # 消费者
│   │       ├── BannerConsumer.java   # Banner消息消费者（主数据）
│   │       └── UserListConsumer.java # 用户列表消息消费者（批次处理）
│   │
│   ├── scheduler/                    # 定时任务
│   │   └── BannerScheduledTask.java # 定时刷新任务（增量+全量）
│   │
│   └── lock/                        # 分布式锁
│       └── RedisDistributedLock.java # Redis分布式锁（带重试机制）
│
├── src/main/resources/
│   ├── application.yml              # 应用配置
│   └── banner-schema.sql            # 数据库脚本
│
└── pom.xml                           # Maven配置
```

---

## 四、数据库表设计

### 4.1 双表设计策略

采用**双表分离设计**，将基础信息与人群包数据分开存储，便于独立管理和优化：

| 表名 | 职责 | 数据量 | 特点 |
|------|------|--------|------|
| `simple_banner` | Banner基础信息（标题、图片、链接、优先级等） | 中等（千级） | 支持索引优化查询 |
| `banner` | 人群包用户关系（bannerId + userId） | 海量（亿级） | 按bannerId分区 |

### 4.2 simple_banner 表结构

| 字段名 | 类型 | 约束 | 说明 |
|--------|------|------|------|
| id | BIGINT | PRIMARY KEY, AUTO_INCREMENT | 自增主键 |
| banner_id | VARCHAR(64) | NOT NULL, UNIQUE | Banner唯一业务ID |
| product_id | VARCHAR(32) | NOT NULL | 业务线标识 |
| title | VARCHAR(128) | NOT NULL | 标题 |
| image_url | VARCHAR(512) | NOT NULL | 图片地址 |
| link_url | VARCHAR(512) | NULL | 跳转链接 |
| priority | INT | NOT NULL, DEFAULT 0 | 优先级（越大越靠前） |
| status | TINYINT | NOT NULL, DEFAULT 1 | 状态：0-禁用，1-启用 |
| start_day | DATE | NOT NULL | 生效开始日期 |
| end_day | DATE | NOT NULL | 生效结束日期 |
| create_time | DATETIME | NOT NULL, DEFAULT CURRENT_TIMESTAMP | 创建时间 |
| update_time | DATETIME | NOT NULL, ON UPDATE CURRENT_TIMESTAMP | 更新时间 |

**索引设计**：
| 索引名 | 字段 | 类型 | 用途 |
|--------|------|------|------|
| uk_banner_id | banner_id | UNIQUE | 唯一约束，快速查找 |
| idx_product_day | product_id, start_day, end_day | NORMAL | 按业务线+日期范围查询 |
| idx_update_time | update_time | NORMAL | 增量刷新时按更新时间查询 |

### 4.3 banner 表结构（人群包）

| 字段名 | 类型 | 约束 | 说明 |
|--------|------|------|------|
| id | BIGINT | PRIMARY KEY, AUTO_INCREMENT | 自增主键 |
| banner_id | VARCHAR(64) | NOT NULL | Banner唯一业务ID |
| user_id | BIGINT | NOT NULL | 用户ID |
| create_time | DATETIME | NOT NULL, DEFAULT CURRENT_TIMESTAMP | 创建时间 |

**索引设计**：
| 索引名 | 字段 | 类型 | 用途 |
|--------|------|------|------|
| idx_banner_id | banner_id | NORMAL | 按bannerId查询用户列表 |

**DDL脚本**：

```sql
CREATE TABLE IF NOT EXISTS `simple_banner` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `banner_id` varchar(64) NOT NULL COMMENT 'Banner唯一业务ID',
  `product_id` varchar(32) NOT NULL COMMENT '业务线标识',
  `title` varchar(128) NOT NULL COMMENT '标题',
  `image_url` varchar(512) NOT NULL COMMENT '图片地址',
  `link_url` varchar(512) DEFAULT NULL COMMENT '跳转链接',
  `priority` int NOT NULL DEFAULT 0 COMMENT '优先级，越大越靠前',
  `status` tinyint NOT NULL DEFAULT 1 COMMENT '状态：0-禁用，1-启用',
  `start_day` date NOT NULL COMMENT '生效开始日期',
  `end_day` date NOT NULL COMMENT '生效结束日期',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `update_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_banner_id` (`banner_id`),
  KEY `idx_product_day` (`product_id`, `start_day`, `end_day`),
  KEY `idx_update_time` (`update_time`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Banner配置表';

CREATE TABLE IF NOT EXISTS `banner` (
  `id` bigint NOT NULL AUTO_INCREMENT COMMENT '自增主键',
  `banner_id` varchar(64) NOT NULL COMMENT 'Banner唯一业务ID',
  `user_id` bigint NOT NULL COMMENT '用户ID',
  `create_time` datetime NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (`id`),
  KEY `idx_banner_id` (`banner_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='Banner人群包表';
```

---

## 五、并发控制设计

### 5.1 分布式锁机制

基于Redis实现分布式锁，解决多实例部署时的并发冲突问题。

**锁配置常量**：

| 常量 | 值 | 说明 |
|------|-----|------|
| LOCK_TIMEOUT_SECONDS | 30 | 锁超时时间，防止死锁 |
| MAX_RETRY_ATTEMPTS | 3 | 最大重试次数 |
| INITIAL_RETRY_INTERVAL_MS | 100 | 初始重试间隔（毫秒） |

**核心API**：

| 方法 | 功能 | 实现说明 |
|------|------|----------|
| `tryLock(String lockKey)` | 尝试获取锁 | 使用 `SETNX` 命令，带过期时间 |
| `tryLockWithRetry(String lockKey)` | 带重试的获取锁 | 指数退避策略（100ms → 300ms → 900ms） |
| `unlock(String lockKey, String lockValue)` | 释放锁 | Lua脚本保证原子性，防止误删 |
| `tryLockAll(List<String> lockKeys)` | 批量获取锁 | 全部成功才返回，失败则回滚已获取的锁 |

**锁使用场景**：

| 场景 | 锁粒度 | 用途 |
|------|--------|------|
| Banner同步 | `banner:{productId}:{date}` | 同一日期的Banner更新互斥 |
| 用户列表刷新 | `bannerId:{bannerId}` | 同一Banner的人群包刷新互斥 |

**核心代码**：

```java
// 获取锁（带重试）
public String tryLockWithRetry(String lockKey) {
    for (int i = 0; i < MAX_RETRY_ATTEMPTS; i++) {
        String lockValue = tryLock(lockKey);
        if (lockValue != null) {
            return lockValue;
        }
        long waitTime = (long) (INITIAL_RETRY_INTERVAL_MS * Math.pow(3, i));
        Thread.sleep(waitTime);
    }
    return null;
}

// 释放锁（Lua脚本保证原子性）
private static final String UNLOCK_SCRIPT =
        "if redis.call('get', KEYS[1]) == ARGV[1] then " +
        "    return redis.call('del', KEYS[1]) " +
        "else " +
        "    return 0 " +
        "end";
```

### 5.2 版本号机制

采用**乐观锁思想**，通过版本号防止旧数据覆盖新数据。

**版本号存储**：

| 存储位置 | Key格式 | 说明 |
|----------|---------|------|
| Redis | `bannerId:{bannerId}:version` | 人群包数据版本号 |

**版本号使用流程**：

```
1. 生产者发送消息时携带版本号
2. 消费者接收消息时校验版本号
3. 仅处理版本号大于当前存储版本的数据
4. 更新数据时同时更新版本号
```

### 5.3 批次消息一致性保证

**问题场景**：用户列表消息分批发送，可能存在部分批次丢失或乱序。

**解决方案**：

| 机制 | 实现 | 作用 |
|------|------|------|
| 批次元信息 | `temp:{bannerId}:{version}:meta` | 记录已接收的批次索引 |
| 临时存储 | `temp:{bannerId}:{version}:batch:{index}` | 临时存储批次数据（2小时过期） |
| 合并切换 | `mergeAndSwitch()` | 全部批次到达后合并，原子切换 |

**流程**：

```
批次消息到达 → 存储到临时区域 → 更新元信息 → 检查是否全部到达
    ├─ 未全部到达：等待下一批
    └─ 全部到达：合并数据 → 分桶存储 → 原子切换 → 清理临时数据
```

---

## 六、核心设计

### 6.1 分桶策略（核心）

#### 6.1.1 分桶算法

```java
// 计算用户所属桶索引
int bucketIndex = userId % totalBuckets;

// 计算总桶数
int totalBuckets = (int) Math.ceil((double) userCount / bucketSize);
```

**关键常量**：

| 常量 | 值 | 说明 |
|------|-----|------|
| BUCKET_SIZE | 1000 | 每个桶的目标用户数（用于计算总桶数） |

#### 6.1.2 分桶流程图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          生产者端（批次发送）                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  用户列表: [1, 5, 10, 15, 20, 25, 100, 200, 1000, 2000] (10人)           │
│  bucketSize = 3 → totalBuckets = 4                                         │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │                    按批次切分 (每批最多1000)                        │  │
│  ├─────────────────────────────────────────────────────────────────────┤  │
│  │  Batch 0: [1, 5, 10]     → 发送: batchIndex=0, totalBatches=4,   │  │
│  │                            totalBuckets=4, userIds=[1,5,10]       │  │
│  │  Batch 1: [15, 20, 25]   → 发送: batchIndex=1, totalBatches=4,   │  │
│  │                            totalBuckets=4, userIds=[15,20,25]     │  │
│  │  Batch 2: [100, 200, 1000] → 发送: batchIndex=2, totalBatches=4, │  │
│  │                            totalBuckets=4, userIds=[100,200,1000] │  │
│  │  Batch 3: [2000]         → 发送: batchIndex=3, totalBatches=4,   │  │
│  │                            totalBuckets=4, userIds=[2000]         │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────┬─────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          消费者端（分桶存储）                                 │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │  阶段1: 接收批次消息，存储到临时区域                                │  │
│  │  ├─ Batch 0 → temp:{bannerId}:v1:batch:0                        │  │
│  │  ├─ Batch 1 → temp:{bannerId}:v1:batch:1                        │  │
│  │  ├─ Batch 2 → temp:{bannerId}:v1:batch:2                        │  │
│  │  └─ Batch 3 → temp:{bannerId}:v1:batch:3                        │  │
│  ├─────────────────────────────────────────────────────────────────────┤  │
│  │  阶段2: 全部批次到达后，按 userId % totalBuckets 分桶             │  │
│  │  ├─ userId=1    → 1 % 4 = 1 → Bucket 1                         │  │
│  │  ├─ userId=5    → 5 % 4 = 1 → Bucket 1                         │  │
│  │  ├─ userId=10   → 10 % 4 = 2 → Bucket 2                        │  │
│  │  ├─ userId=15   → 15 % 4 = 3 → Bucket 3                        │  │
│  │  ├─ userId=20   → 20 % 4 = 0 → Bucket 0                        │  │
│  │  ├─ userId=25   → 25 % 4 = 1 → Bucket 1                        │  │
│  │  ├─ userId=100  → 100 % 4 = 0 → Bucket 0                       │  │
│  │  ├─ userId=200  → 200 % 4 = 0 → Bucket 0                       │  │
│  │  ├─ userId=1000 → 1000 % 4 = 0 → Bucket 0                      │  │
│  │  └─ userId=2000 → 2000 % 4 = 0 → Bucket 0                      │  │
│  ├─────────────────────────────────────────────────────────────────────┤  │
│  │  阶段3: 原子切换到正式数据                                       │  │
│  │  ├─ 删除旧桶数据                                                 │  │
│  │  ├─ 写入新桶数据                                                 │  │
│  │  ├─ 更新totalBuckets                                            │  │
│  │  └─ 更新version                                                 │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
│  Redis存储结果:                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │  bannerId:{bannerId}:bucketIndex:0 → {20, 100, 200, 1000, 2000}  │  │
│  │  bannerId:{bannerId}:bucketIndex:1 → {1, 5, 25}                  │  │
│  │  bannerId:{bannerId}:bucketIndex:2 → {10}                         │  │
│  │  bannerId:{bannerId}:bucketIndex:3 → {15}                         │  │
│  │  bannerId:{bannerId}:totalBuckets → 4                             │  │
│  │  bannerId:{bannerId}:version → 1                                 │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────┬─────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                            C端查询（按桶定位）                               │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  查询 userId=1000                                                           │
│                                                                             │
│  ┌─────────────────────────────────────────────────────────────────────┐  │
│  │  1. 获取 totalBuckets = 4                                          │  │
│  │  2. bucketIndex = 1000 % 4 = 0                                     │  │
│  │  3. SISMEMBER bannerId:{bannerId}:bucketIndex:0 1000               │  │
│  │  4. 结果: 存在 → 返回该Banner                                       │  │
│  └─────────────────────────────────────────────────────────────────────┘  │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

#### 6.1.3 批次与分桶的区别

| 概念 | 定义 | 作用 | 位置 |
|------|------|------|------|
| **批次 (Batch)** | 生产者将大量用户ID分成多个消息发送，每批最多1000个 | 避免单条消息过大，提高传输效率 | 生产者端 |
| **桶 (Bucket)** | 消费者将用户ID按哈希分配到不同的Set中存储 | 实现O(1)时间复杂度的用户归属查询 | 消费者端 |

### 6.2 Redis存储设计

#### 6.2.1 主数据存储

| 项目 | 设计 |
|------|------|
| **Key格式** | `banner:{productId}:{date}` |
| **数据结构** | Hash |
| **Field** | `bannerId` |
| **Value** | SimpleBannerInfo JSON字符串 |
| **示例** | `banner:prod01:20260526` |

#### 6.2.2 人群包存储

| 项目 | 设计 |
|------|------|
| **Key格式** | `bannerId:{bannerId}:bucketIndex:{index}` |
| **数据结构** | Set |
| **Value** | userId |
| **示例** | `bannerId:banner_001:bucketIndex:0` |

#### 6.2.3 元数据存储

| 项目 | Key格式 | 数据结构 | 用途 |
|------|---------|----------|------|
| 总桶数 | `bannerId:{bannerId}:totalBuckets` | String | 查询时确定取余的模值 |
| 版本号 | `bannerId:{bannerId}:version` | String | 版本号机制，防止旧数据覆盖 |

#### 6.2.4 临时批次数据

| 项目 | Key格式 | 数据结构 | 用途 |
|------|---------|----------|------|
| 批次数据 | `temp:{bannerId}:{version}:batch:{batchIndex}` | Set | 临时存储批次数据（2小时过期） |
| 批次元信息 | `temp:{bannerId}:{version}:meta` | String(JSON) | 记录已接收的批次索引 |

#### 6.2.5 分布式锁存储

| 项目 | Key格式 | 数据结构 | 用途 |
|------|---------|----------|------|
| 锁 | `lock:banner:{productId}:{date}` | String | 分布式锁（UUID值，30秒过期） |

### 6.3 Kafka消息设计

#### 6.3.1 消息主题

| 主题 | 用途 | 消息类型 | 分区策略 |
|------|------|----------|----------|
| `banner-topic` | 主消息UPDATE（基础信息） | SimpleBannerInfo | 按productId分区 |
| `banner-delete-topic` | 主消息DELETE（基础信息） | SimpleBannerInfo | 按productId分区 |
| `banner-userlist-batch-topic` | 分桶消息UPDATE（人群包） | UserListBatchMessage | 按bannerId分区 |
| `banner-userlist-delete-topic` | 分桶消息DELETE（人群包） | UserListBatchMessage | 按bannerId分区 |

**消费者组**：`banner-consumer-group`

#### 6.3.2 批次消息 UserListBatchMessage

```json
{
  "bannerId": "banner_001",
  "version": 1,
  "batchIndex": 0,
  "totalBatches": 10,
  "totalBuckets": 10,
  "userIds": [1001, 1002, 1003, ...]
}
```

**字段说明**：

| 字段 | 类型 | 说明 |
|------|------|------|
| bannerId | String | Banner唯一业务ID |
| version | Long | 版本号（用于版本校验） |
| batchIndex | Integer | 当前批次索引（从0开始） |
| totalBatches | Integer | 总批次数 |
| totalBuckets | Integer | 总桶数（预先计算） |
| userIds | List<Long> | 该批次的用户ID列表（每批≤1000） |

---

## 七、核心服务模块设计

### 7.1 BannerSyncService（同步服务）

**职责**：负责Banner主数据的同步，包括Kafka消息处理和数据库兜底刷新。

**核心方法**：

| 方法 | 参数 | 返回值 | 功能说明 |
|------|------|--------|----------|
| `syncBannerFromKafka` | SimpleBannerInfo | void | 处理Kafka同步消息 |
| `deleteBannerFromKafka` | String bannerId | void | 处理Kafka删除消息 |
| `refreshFromDatabase` | String productId, String date | void | 从数据库刷新指定日期的Banner |

**同步流程**：

```
Kafka消息 → 版本校验 → 计算日期范围 → 尝试获取分布式锁 → 更新Redis → 释放锁
```

### 7.2 BannerQueryService（查询服务）

**职责**：为C端提供Banner查询接口，支持人群包过滤。

**核心方法**：

| 方法 | 参数 | 返回值 | 功能说明 |
|------|------|--------|----------|
| `getBannersByDate` | String productId, String date | List<BannerInfo> | 查询指定日期的Banner列表 |
| `getBannersByDateAndUserId` | String productId, String date, Long userId | List<BannerInfo> | 查询指定日期且用户在人群包内的Banner |

**查询流程**：

```
请求 → 从Redis读取Hash → 遍历Banner → 人群包过滤 → 按优先级排序 → 返回
```

### 7.3 BannerUserListService（用户列表服务）

**职责**：管理人群包用户列表的同步和刷新。

**核心方法**：

| 方法 | 参数 | 返回值 | 功能说明 |
|------|------|--------|----------|
| `syncUserListBatch` | bannerId, version, batchIndex, totalBatches, totalBuckets, userIds | void | 处理批次消息 |
| `deleteUserList` | String bannerId | void | 删除人群包数据 |
| `refreshFromDatabase` | String bannerId | void | 从数据库刷新人群包 |

### 7.4 UserBucketCacheManager（用户桶缓存管理）

**职责**：管理Redis中的人群包分桶数据。

**核心方法**：

| 方法 | 参数 | 返回值 | 功能说明 |
|------|------|--------|----------|
| `setBucket` | bannerId, bucketIndex, userIds | void | 设置桶数据 |
| `deleteAllBuckets` | String bannerId | void | 删除所有桶 |
| `isUserInBucket` | bannerId, bucketIndex, userId | boolean | 检查用户是否在桶中 |
| `atomicSwitch` | bannerId, totalBuckets, bucketData, version | void | 原子切换数据 |
| `mergeAndSwitch` | bannerId, version, totalBuckets | void | 合并临时批次并切换 |
| `setTempBatchData` | bannerId, version, batchIndex, userIds | void | 存储临时批次数据 |
| `isAllBatchesReceived` | bannerId, version, totalBatches | boolean | 检查是否所有批次到达 |

---

## 八、定时任务设计

### 8.1 任务配置

| 任务 | 调度频率 | 触发时间 | 用途 |
|------|----------|----------|------|
| incrementalRefresh | 每5分钟 | 固定速率 | 增量刷新最近N分钟更新的数据 |
| fullConsistencyCheck | 每30分钟 | 固定速率 | 全量一致性检查 |

### 8.2 增量刷新流程

```
获取最近10分钟更新的Banner → 按productId分组 → 逐个刷新Banner和人群包
```

**时间窗口配置**：`INCREMENTAL_WINDOW_MINUTES = 10`

### 8.3 全量一致性检查流程

```
查询所有启用状态的Banner → 按productId分组 → 逐个刷新Banner和人群包
```

**锁机制**：每次刷新前尝试获取分布式锁，防止多实例重复刷新。

---

## 九、接口文档

### 9.1 C端查询接口

#### 获取指定日期的Banner列表（支持人群包过滤）

**请求**：
```
GET /api/banners?productId=prod01&date=20260526&userId=1001
```

**参数**：
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| productId | String | 是 | 业务线标识 |
| date | String | 否 | 日期（YYYYMMDD），默认今天 |
| userId | Long | 否 | 用户ID，用于人群包过滤 |

**响应**：
```json
{
  "code": 200,
  "msg": "success",
  "data": {
    "productId": "prod01",
    "date": "20260526",
    "banners": [
      {
        "bannerId": "banner_001",
        "title": "618大促Banner",
        "imageUrl": "https://example.com/image.jpg",
        "linkUrl": "https://example.com/activity",
        "priority": 100
      }
    ]
  }
}
```

### 9.2 内部接口（管理端调用）

#### 同步Banner数据（管理端推送）

**请求**：
```
POST /api/banners/sync
Content-Type: application/json

{
  "bannerId": "banner_001",
  "productId": "prod01",
  "title": "618大促Banner",
  "imageUrl": "https://example.com/image.jpg",
  "linkUrl": "https://example.com/activity",
  "priority": 100,
  "status": 1,
  "startDay": "2026-05-25",
  "endDay": "2026-06-05"
}
```

**响应**：
```json
{
  "code": 200,
  "msg": "success",
  "data": null
}
```

#### 删除Banner数据

**请求**：
```
DELETE /api/banners/{bannerId}
```

**响应**：
```json
{
  "code": 200,
  "msg": "success",
  "data": null
}
```

---

## 十、版本历史

| 版本 | 日期 | 说明 |
|------|------|------|
| V1.0 | 2026-05 | 初版设计，单表结构 |
| V2.0 | 2026-05 | 双表设计，支持人群包功能，批次发送+分桶存储策略，临时批次合并后原子切换 |

---

## 十一、配置说明

### 11.1 核心配置项

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `banner.bucket-size` | 每个桶的目标用户数 | 1000 |
| `banner.lock.timeout-seconds` | 分布式锁超时时间 | 30 |
| `banner.lock.max-retry-attempts` | 锁获取最大重试次数 | 3 |
| `banner.scheduler.incremental-window-minutes` | 增量刷新时间窗口 | 10 |
| `banner.scheduler.incremental-interval-seconds` | 增量刷新间隔 | 300 |
| `banner.scheduler.consistency-check-interval-seconds` | 一致性检查间隔 | 1800 |

### 11.2 Kafka配置

| 配置项 | 说明 |
|--------|------|
| `spring.kafka.bootstrap-servers` | Kafka集群地址 |
| `spring.kafka.consumer.group-id` | 消费者组ID |
| `spring.kafka.consumer.auto-offset-reset` | 偏移量重置策略 |
| `spring.kafka.producer.key-serializer` | Key序列化器 |
| `spring.kafka.producer.value-serializer` | Value序列化器 |

### 11.3 Redis配置

| 配置项 | 说明 |
|--------|------|
| `spring.data.redis.host` | Redis主机 |
| `spring.data.redis.port` | Redis端口 |
| `spring.data.redis.timeout` | 连接超时时间 |
| `spring.data.redis.jedis.pool.max-active` | 最大活跃连接数 |
| `spring.data.redis.jedis.pool.max-idle` | 最大空闲连接数 |
| `spring.data.redis.jedis.pool.min-idle` | 最小空闲连接数 |
