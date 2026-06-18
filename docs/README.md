# Banner消费端系统 - V2.0设计文档

## 一、业务背景

### 1.1 业务场景

运营端（B端）管理系统负责创建和管理Banner资源位，每个Banner可关联一个人群包（指定可见用户列表）。C端用户在App内请求Banner时，需要根据业务线、日期和用户ID快速返回命中的Banner列表。

### 1.2 核心诉求

| 诉求 | 说明 |
|------|------|
| **高性能读取** | C端请求量大，需毫秒级响应 |
| **人群包支持** | Banner可指定可见用户列表，支持数十万级用户 |
| **数据最终一致** | 运营端修改后，C端需在秒级内感知变更 |
| **高可用** | 缓存故障时仍可降级提供服务 |

### 1.3 系统定位

本系统是**Banner缓存同步与查询服务**，不写入业务数据，职责为：

- 接收运营端Kafka消息，RPC拉取完整数据，同步到Redis缓存
- 通过Caffeine本地缓存（30秒TTL）+ Redis二级缓存为C端提供高性能查询
- 通过XXL-Job定时任务保证缓存最终一致性

### 1.4 核心特点

| 特性 | 说明 |
|------|------|
| **高性能读** | Caffeine本地缓存（30秒TTL）+ Redis二级缓存，毫秒级响应 |
| **人群包支持** | 动态分桶存储用户ID列表，解决BigKey问题 |
| **消息幂等** | Redis SET NX + 过期Key，无数据库依赖 |
| **缓存一致性** | Caffeine短TTL自动失效，无Pub/Sub广播 |
| **分布式锁** | 不重试，获取不到锁直接返回，与定时任务共用同一锁key |
| **代码规范** | 两层架构（Controller → Service），方法内分点注释 |

---

## 二、整体架构

### 2.1 架构图

```
┌──────────────────────────────────────────────────────────────────────┐
│                         运营端（B端）                                │
│   写入 simple_banner + banner_user_list → 发送MQ(id + type)         │
└──────────────────────────────┬───────────────────────────────────────┘
                               │
                               ▼
                      ┌─────────────────┐
                      │ banner-sync-topic│   单一Topic，type区分消息类型
                      └────────┬────────┘
                               │
                               ▼
┌──────────────────────────────────────────────────────────────────────┐
│                      Banner消费端服务                                │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │                    BannerSyncConsumer                          │  │
│  │  ① 幂等检查(Redis SET NX)                                      │  │
│  │  ② DELETE类型：设置status=0（软删除）                           │  │
│  │  ③ 获取分布式锁（不重试，与定时任务共用）                        │  │
│  │  ④ RPC调用运营端获取Banner基本信息                              │  │
│  │  ⑤ 分页RPC获取用户列表，分桶写入Redis                          │  │
│  │  ⑥ 更新Banner详情和日期索引到Redis                             │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                               │                                      │
│              ┌────────────────┼────────────────┐                     │
│              ▼                ▼                ▼                     │
│       ┌───────────┐   ┌───────────┐   ┌───────────────┐             │
│       │   Redis   │   │ Caffeine  │   │    XXL-Job    │             │
│       │ (主缓存)  │   │ (本地缓存)│   │  (定时任务)   │             │
│       └───────────┘   └───────────┘   └───────────────┘             │
└──────────────────────────────────────────────────────────────────────┘
                               │
                               ▼
                      ┌─────────────────┐
                      │   C端用户请求    │
                      │ POST /api/banners│
                      └─────────────────┘
```

### 2.2 技术选型

| 组件 | 选型 | 说明 |
|------|------|------|
| 消息队列 | Kafka | 单一Topic，type字段区分消息类型 |
| 分布式缓存 | Redis | 主缓存，分桶存储人群包 |
| 本地缓存 | Caffeine | 二级缓存，30秒TTL自动失效 |
| 缓存一致性 | 短TTL自动失效 | 不使用Pub/Sub广播 |
| 分布式锁 | Redis + Lua | 不重试，获取不到锁直接返回 |
| 定时任务 | XXL-Job | 增量同步 + 全量一致性检查 |
| 幂等存储 | Redis SET NX | 无数据库依赖 |
| RPC调用 | RestTemplate | 调用运营端获取数据 |

### 2.3 核心模块职责

| 模块 | 职责 | 关键组件 |
|------|------|----------|
| **common** | 公共模块：常量、枚举、DTO、工具类 | `BannerConstants`, `MessageType`, `DateUtil`, `JsonUtil` |
| **client** | RPC客户端：调用运营端获取数据 | `BannerOperationClient` |
| **mq** | 消息队列：单一Topic消费者 | `BannerSyncConsumer` |
| **service** | 业务逻辑：缓存管理、查询服务 | `BannerService` |
| **controller** | 控制层：C端查询接口 | `BannerController` |
| **config** | 配置类：Redis、Kafka、Caffeine | `RedisConfig`, `CaffeineCacheConfig`, `KafkaConfig` |
| **scheduler** | 定时任务：增量同步+全量一致性检查 | `BannerScheduledTask` |
| **lock** | 分布式锁 | `RedisDistributedLock` |

---

## 三、核心流程

### 3.1 缓存同步流程（MQ消费）

```
运营端写DB完成
    │
    ▼
Kafka Topic: banner-sync-topic
    │  消息: {messageId: UUID, id: 123, type: 1}
    ▼
BannerSyncConsumer.consume()
    │
    ├─① 幂等检查
    │    Redis SET NX idempotent:messageId:{messageId} "1" EX 7天
    │    ├─ 成功 → 继续
    │    └─ 失败 → 已消费，跳过
    │
    ├─② type=DELETE → 设置status=0（软删除，不删人群包），结束
    │
    ├─③ 获取分布式锁（不重试）
    │    tryLock("bannerLock:bannerId:123")
    │    ├─ 成功 → 继续
    │    └─ 失败 → 返回（稍后重试或定时任务兜底）
    │
    ├─④ RPC获取Banner基本信息
    │    BannerOperationClient.getBannerById(123) → Banner
    │
    ├─⑤ 分页RPC获取用户列表
    │    while (hasNext):
    │        BannerOperationClient.getUserIdsByPage(123, page, 1000)
    │        addUserToBuckets(123, bucketCount, userIds)
    │
    ├─⑥ 更新缓存
    │    ├─ SET bannerInfo:bannerId:123 → Banner JSON
    │    ├─ 遍历startTime到endTime的日期，添加到日期索引
    │    │    SADD banners:productId:{productId}:date:{date} 123
    │    └─ 删除旧用户桶（步骤⑤前已执行）
    │
    └─⑦ 释放锁
         unlock("bannerLock:bannerId:123", lockValue)
```

### 3.2 查询流程

```
C端请求: POST /api/banners
    Body: {"productId": 1, "date": "20260618", "userId": 10086}
    │
    ▼
BannerController.getBanners(BannerQueryRequest)
    │
    ▼
BannerService.queryBanners(1, "20260618", 10086)
    │
    ├─① 获取该产品+日期下的所有活跃Banner
    │    getActiveBanners(1, "20260618")
    │    ├─ 先查本地缓存：productDateLocalCache.get(productId+date)
    │    ├─ 未命中 → Redis SMEMBERS banners:productId:1:date:20260618
    │    ├─ 更新本地缓存
    │    ├─ 根据ID逐个查Banner详情（先查本地缓存，未命中查Redis）
    │    └─ 过滤status=1的活跃Banner，按优先级降序排列
    │
    └─② 人群包过滤
         遍历每个Banner，调用isUserInBucket(bannerId, 10086)
         ├─ 获取Banner详情（含bucketCount）
         ├─ bucketIndex = 10086 % bucketCount
         └─ SISMEMBER bannerUsers:bannerId:{bannerId}:bucketIndex:{bucketIndex} "10086"
    │
    ▼
返回过滤后的Banner列表
```

### 3.3 定时任务流程

```
XXL-Job调度中心
    │
    ├─ incrementalSyncJob（增量同步，每5分钟）
    │    │
    │    ▼
    │    RPC: BannerOperationClient.getIncrementalUpdatedIds(sinceTime)
    │    │
    │    ▼
    │    逐个调用 syncBanner(bannerId)（带分布式锁）
    │
    └─ fullConsistencyCheckJob（全量一致性检查，每天凌晨2点）
         │
         ▼
         RPC: BannerOperationClient.getAllActiveBannerIds()
         │
         ▼
         逐个检查：
         ├─ 上游存在 → syncBanner(bannerId)
         └─ 上游不存在 → setBannerInactive(bannerId)（软删除）
```

---

## 四、对外暴露的HTTP接口

### 4.1 查询Banner列表

**请求**

```
POST /api/banners
Content-Type: application/json
```

**请求参数**

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| productId | Integer | 是 | 业务线ID |
| date | String | 是 | 日期，格式yyyyMMdd |
| userId | Long | 是 | 用户ID，用于人群包过滤 |

**请求示例**

```json
{
    "productId": 1,
    "date": "20260618",
    "userId": 10086
}
```

**响应结构**

```json
{
    "code": 200,
    "msg": "success",
    "data": {
        "productId": 1,
        "date": "20260618",
        "banners": [
            {
                "id": 123,
                "title": "暑期促销",
                "imageUrl": "https://cdn.example.com/banner/123.png",
                "linkUrl": "https://m.example.com/promotion/123",
                "priority": 10
            },
            {
                "id": 456,
                "title": "新品上线",
                "imageUrl": "https://cdn.example.com/banner/456.png",
                "linkUrl": "https://m.example.com/new/456",
                "priority": 5
            }
        ]
    }
}
```

**响应字段说明**

| 字段 | 类型 | 说明 |
|------|------|------|
| code | Integer | 状态码，200=成功，500=失败 |
| msg | String | 状态描述 |
| data.productId | Integer | 业务线ID |
| data.date | String | 查询日期 |
| data.banners | Array | Banner列表，按priority降序排列 |
| data.banners[].id | Long | Banner ID |
| data.banners[].title | String | 标题 |
| data.banners[].imageUrl | String | 图片地址 |
| data.banners[].linkUrl | String | 跳转链接 |
| data.banners[].priority | Integer | 优先级，数值越大越靠前 |

**错误响应**

```json
{
    "code": 500,
    "msg": "internal server error",
    "data": null
}
```

---

## 五、MQ消息协议

### 5.1 Topic

| Topic | 说明 |
|-------|------|
| banner-sync-topic | 统一消息Topic，通过type字段区分操作类型 |

### 5.2 消息体

```json
{
    "messageId": "550e8400-e29b-41d4-a716-446655440000",
    "id": 123,
    "type": 1
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| messageId | String | 消息唯一ID（UUID），用于幂等 |
| id | Long | Banner的数据库ID |
| type | Integer | 消息类型：1=同步，2=删除 |

### 5.3 消息类型

| type | 枚举 | 说明 | 消费者处理 |
|------|------|------|-----------|
| 1 | SYNC | Banner新增/更新 | RPC获取数据 → 更新缓存 |
| 2 | DELETE | Banner删除 | 设置status=0（软删除），不删除人群包 |

### 5.4 幂等机制

```
消费者收到消息
    │
    ▼
Redis SET NX idempotent:messageId:{messageId} "1" EX 7天
    │
    ├─ SET成功 → 首次消费，继续处理
    │
    └─ SET失败 → 同一messageId已存在，说明已消费过，跳过
```

**设计要点**：
- 每条MQ消息携带唯一`messageId`（UUID），由生产者生成
- 幂等标记存储在Redis中，7天自动过期
- 无数据库依赖，简化部署

---

## 六、Redis存储设计

### 6.1 Key规范

所有Key通过`String.format`直接构造：

| Key模板 | 示例 | 类型 | 说明 |
|---------|------|------|------|
| `bannerInfo:bannerId:%d` | `bannerInfo:bannerId:123` | String | Banner基本信息JSON |
| `bannerUsers:bannerId:%d:bucketIndex:%d` | `bannerUsers:bannerId:123:bucketIndex:0` | Set | 用户ID分桶 |
| `banners:productId:%d:date:%s` | `banners:productId:1:date:20260618` | Set | 按产品+日期索引Banner ID |
| `bannerLock:bannerId:%d` | `bannerLock:bannerId:123` | String | 分布式锁（定时任务+消费者共用） |
| `idempotent:messageId:%s` | `idempotent:messageId:uuid-xxx` | String | 幂等标记（7天过期） |

### 6.2 分桶策略（取模定位，O(1)查询）

**解决的问题**：人群包可能包含数十万甚至上百万用户ID，直接存为一个Redis Set会导致BigKey，阻塞Redis。

| 配置项 | 值 | 说明 |
|--------|-----|------|
| USER_BUCKET_SIZE | 1000 | RPC分页大小 |
| MAX_RPC_PAGE_COUNT | 1000 | 最大分页数量限制 |

**桶数量**：由Banner的`bucketCount`字段动态决定。

**查询流程（O(1)复杂度）**：

```
isUserInBucket(bannerId, userId):
    → 获取Banner详情（含bucketCount）
    → bucketIndex = userId % bucketCount
    → SISMEMBER bannerUsers:bannerId:{bannerId}:bucketIndex:{bucketIndex} userId
```

**写入流程**：

```
遍历每个userId
    → bucketIndex = userId % bucketCount
    → SADD bannerUsers:bannerId:{bannerId}:bucketIndex:{bucketIndex} userId
```

### 6.3 日期索引设计

Banner有`startTime`和`endTime`字段，表示有效期。同步时为有效期内每一天创建索引：

```
同步时：
    → 计算日期范围：startTime到endTime之间的所有日期
    → 对每个日期date：
        SADD banners:productId:{productId}:date:{date} {bannerId}

查询时：
    → SMEMBERS banners:productId:{productId}:date:{queryDate}
    → 返回当天有效的Banner ID列表
```

---

## 七、数据库表设计（运营端）

### 7.1 simple_banner 表

| 字段名 | 类型 | 约束 | 说明 |
|--------|------|------|------|
| id | BIGINT | PRIMARY KEY, AUTO_INCREMENT | 自增主键（即bannerId） |
| product_id | INT | NOT NULL | 业务线标识 |
| title | VARCHAR(128) | NOT NULL | 标题 |
| image_url | VARCHAR(512) | NOT NULL | 图片地址 |
| link_url | VARCHAR(512) | NULL | 跳转链接 |
| priority | INT | NOT NULL, DEFAULT 0 | 优先级 |
| status | INT | NOT NULL, DEFAULT 1 | 状态：0-禁用/删除，1-启用 |
| start_time | BIGINT | NOT NULL | 生效开始时间（时间戳） |
| end_time | BIGINT | NOT NULL | 生效结束时间（时间戳） |
| bucket_count | INT | DEFAULT 1 | 人群包分桶数量 |
| create_time | BIGINT | NOT NULL | 创建时间（时间戳） |
| update_time | BIGINT | NOT NULL | 更新时间（时间戳） |

### 7.2 banner_user_list 表

| 字段名 | 类型 | 约束 | 说明 |
|--------|------|------|------|
| id | BIGINT | PRIMARY KEY, AUTO_INCREMENT | 自增主键 |
| banner_id | BIGINT | NOT NULL | 关联simple_banner.id |
| user_id | BIGINT | NOT NULL | 用户ID |
| create_time | BIGINT | NOT NULL | 创建时间（时间戳） |

**索引**：`UNIQUE KEY uk_banner_user (banner_id, user_id)`

---

## 八、并发控制

### 8.1 分布式锁

消费者和定时任务对同一Banner使用相同的锁key：

```
lockKey = "bannerLock:bannerId:{bannerId}"

tryLock(lockKey)
    ├─ SET NX，TTL=30秒
    └─ 不重试，获取失败直接返回

unlock(lockKey, lockValue)
    └─ Lua脚本原子释放（校验lockValue后删除）
```

### 8.2 并发场景

| 场景 | 是否会并发 | 处理方式 |
|------|-----------|----------|
| 消费者之间 | 否（同一Kafka分区顺序消费） | 无需额外处理 |
| 定时任务之间 | 否（XXL-Job默认单机执行） | 分布式锁保障多机部署安全 |
| 定时任务 vs 消费者 | 是 | 共用同一锁key |

### 8.3 缓存更新原子性

在分布式锁保护下，Banner信息和用户列表作为一个整体更新：

1. 删除旧用户桶
2. 分页RPC获取用户列表并分桶写入
3. 更新Banner详情缓存
4. 更新日期索引（为有效期内每一天创建索引）

---

## 九、定时任务

使用XXL-Job调度平台，`@XxlJob`注解：

| 任务 | Handler | Cron | 说明 |
|------|---------|------|------|
| 增量同步 | `incrementalSyncJob` | `0 */5 * * * ?` | 每5分钟同步最近更新的Banner |
| 全量一致性检查 | `fullConsistencyCheckJob` | `0 0 2 * * ?` | 每天凌晨2点检查所有Banner数据一致性 |

---

## 十、相关配置

### 10.1 application.yml

```yaml
server:
  port: 8080

spring:
  data:
    redis:
      host: localhost
      port: 6379
      database: 0
  kafka:
    bootstrap-servers: localhost:9092
    consumer:
      group-id: banner-consumer-group
      auto-offset-reset: earliest
      enable-auto-commit: true
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer

xxl:
  job:
    admin:
      addresses: http://localhost:8080/xxl-job-admin
    executor:
      appname: banner-consumer
      port: 9999

logging:
  level:
    com.banner: INFO
    org.springframework.kafka: INFO
```

### 10.2 配置项说明

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| `server.port` | 8080 | 服务端口 |
| `spring.data.redis.host` | localhost | Redis地址 |
| `spring.data.redis.port` | 6379 | Redis端口 |
| `spring.kafka.bootstrap-servers` | localhost:9092 | Kafka地址 |
| `spring.kafka.consumer.group-id` | banner-consumer-group | 消费者组 |
| `xxl.job.admin.addresses` | http://localhost:8080/xxl-job-admin | XXL-Job调度中心地址 |
| `xxl.job.executor.appname` | banner-consumer | 执行器名称 |

### 10.3 Caffeine本地缓存配置

| 参数 | 值 | 说明 |
|------|-----|------|
| maximumSize | 10000 | 最大缓存条目数 |
| expireAfterWrite | 30秒 | 写入后过期时间 |

---

## 十一、项目文件目录

```
src/main/java/com/banner/
├── BannerApplication.java                     # 启动类

├── client/                                    # RPC客户端
│   └── BannerOperationClient.java             # 调用运营端获取Banner数据（含分页获取用户列表）

├── common/                                    # 公共模块
│   ├── constant/
│   │   └── BannerConstants.java               # 常量（Topic、锁、分桶配置）
│   ├── dto/
│   │   ├── ApiResponse.java                   # 通用API响应
│   │   ├── Banner.java                        # Banner数据类（status, bucketCount）
│   │   ├── BannerMessage.java                 # MQ消息体（messageId + id + type）
│   │   ├── BannerQueryRequest.java            # 查询请求
│   │   └── BannerQueryResponse.java           # 查询响应
│   ├── enums/
│   │   └── MessageType.java                   # 消息类型枚举（SYNC=1, DELETE=2）
│   ├── param/
│   │   ├── BannerId.java                      # 类型包装类
│   │   ├── ProductId.java
│   │   ├── UserId.java
│   │   └── BannerDate.java
│   └── util/
│       ├── DateUtil.java                      # 日期工具（日期范围计算）
│       └── JsonUtil.java                      # JSON工具

├── config/                                    # 配置类
│   ├── CaffeineCacheConfig.java               # Caffeine本地缓存配置（30秒TTL）
│   ├── KafkaConfig.java                       # Kafka配置
│   └── RedisConfig.java                       # Redis配置

├── controller/                                # 控制层
│   └── BannerController.java                  # C端查询接口（POST /api/banners）

├── lock/                                      # 分布式锁
│   └── RedisDistributedLock.java              # Redis分布式锁（SET NX + Lua释放，不重试）

├── mq/                                        # 消息队列
│   └── consumer/
│       └── BannerSyncConsumer.java            # 统一消费者（幂等+分布式锁+RPC+分桶）

├── scheduler/                                 # 定时任务
│   └── BannerScheduledTask.java               # @XxlJob增量同步+全量一致性检查

└── service/                                   # 服务层（业务逻辑 + 数据访问）
    └── BannerService.java                     # Banner服务（查询、同步、人群包管理）
```
