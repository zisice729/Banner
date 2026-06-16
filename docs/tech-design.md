# Banner消费端系统 - 技术方案

## 一、业务背景

### 1.1 业务需求

Banner消费端系统是为了解决以下业务问题而设计：

| 问题 | 说明 | 影响 |
|------|------|------|
| **Banner展示不一致** | 运营端修改Banner后，C端无法及时看到最新内容 | 用户体验差 |
| **高并发查询压力** | 直接查询数据库无法支撑C端高并发请求 | 数据库过载 |
| **数据一致性保障** | 需要确保Banner数据在多实例部署下保持一致 | 数据混乱 |
| **定时刷新机制** | 需要定期检查并同步数据库中的Banner数据 | 数据过期 |

### 1.2 核心功能

| 功能 | 描述 |
|------|------|
| **实时数据同步** | 接收Kafka消息，实时同步Banner数据到Redis缓存 |
| **高并发查询** | C端通过HTTP接口查询Redis缓存，毫秒级响应 |
| **数据删除处理** | 支持通过Kafka消息删除Banner数据 |
| **定时数据刷新** | 通过定时任务从数据库全量/增量同步数据，保障最终一致性 |
| **分布式锁** | 多实例部署时保证缓存更新的原子性 |

---

## 二、架构设计

### 2.1 整体架构图

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                             外部系统                                        │
│  ┌─────────────────────────┐    ┌─────────────────────────────────────────┐ │
│  │    运营管理系统          │    │              C端用户                   │ │
│  │  发送Kafka消息          │    │       HTTP查询请求                      │ │
│  └───────────┬─────────────┘    └──────────────────────┬──────────────────┘ │
│              │                                         │                     │
└──────────────┼─────────────────────────────────────────┼─────────────────────┘
               │                                         │
               ▼                                         ▼
┌─────────────────────────┐    ┌─────────────────────────────────────────────┐
│     Kafka Topic         │    │              API Gateway                   │
│   banner-topic          │    │  ├─ 参数校验 (productId, date格式)          │
│   (UPDATE/DELETE)       │    │  ├─ 输入清洗                                │
└───────────┬─────────────┘    └──────────────────────┬──────────────────────┘
            │                                         │
            ▼                                         ▼
┌─────────────────────────┐    ┌─────────────────────────────────────────────┐
│      BannerConsumer     │    │           BannersController                 │
│  ├─ JSON反序列化        │    │  ├─ @Valid 参数校验                        │
│  ├─ 幂等性校验          │    │  ├─ 业务逻辑分发                           │
│  │   (bannerId+version) │    └──────────────────────┬──────────────────────┘
│  └─ 异常抛出重试        │                             │
└───────────┬─────────────┘                             ▼
            │                           ┌───────────────────────────────────┐
            ▼                           │         BannerService             │
┌─────────────────────────┐             │  ├─ getBannersByDate()           │
│    BannerServiceImpl    │             │  └─ syncBannerFromKafka()         │
│  ├─ 分布式锁获取        │             └──────────────────┬────────────────┘
│  │   (UUID+Lua脚本)     │                                │
│  ├─ 日期范围计算        │              ┌─────────────────┴─────────────────┐
│  ├─ UPDATE/DELETE分支   │              ▼                                   ▼
│  └─ 锁释放/异常处理     │   ┌─────────────────┐                   ┌─────────┐
└───────────┬─────────────┘   │BannerCacheManager│                   │ Banner  │
            │                 │  ├─ 版本号校验   │                   │Repository│
            ▼                 │  ├─ XSS过滤      │                   │  MySQL  │
┌─────────────────────────┐   │  ├─ 缓存读写     │                   │ 查询    │
│   BannerCacheManager    │   │  └─ 数据刷新     │                   └─────────┘
│  ├─ setBanner()         │   └────────┬────────┘
│  ├─ deleteBanner()      │            │
│  ├─ getBannersByDate()  │            ▼
│  └─ refreshBanners()    │   ┌─────────────────┐
└───────────┬─────────────┘   │    Redis缓存     │
            │                 │ ├─ Hash存储      │
            ▼                 │ ├─ 分布式锁      │
┌─────────────────────────┐   │ └─ 版本号检查    │
│          Redis          │   └─────────────────┘
│  ├─ banner:{productId}  │
│  │   :{date} (Hash)     │
│  ├─ lock:banner:{productId} │
│  │   :{date} (String)   │
│  └─ 幂等性记录 (MySQL)  │
└─────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                            安全防护层                                      │
│  ┌─────────────┐   ┌─────────────┐   ┌─────────────┐   ┌─────────────┐    │
│  │ 参数校验     │   │ XSS过滤     │   │ 协议白名单   │   │ 幂等性保障  │    │
│  │ @Valid      │   │ HTML转义    │   │ http/https  │   │ 版本号去重  │    │
│  └─────────────┘   └─────────────┘   └─────────────┘   └─────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 核心模块职责

| 模块 | 职责 | 核心类 |
|------|------|--------|
| **controller** | HTTP接口层，处理C端查询请求 | `BannersController` |
| **service** | 业务逻辑层，处理Banner查询和同步 | `BannerService`, `BannerServiceImpl` |
| **cache** | 缓存管理层，Redis操作封装（含版本号校验、XSS过滤） | `BannerCacheManager`, `BannerCacheManagerImpl` |
| **repository** | 数据访问层，数据库操作（含幂等性记录） | `BannerRepository`, `MessageProcessRecordRepository` |
| **mq/consumer** | Kafka消费者，接收消息并同步（含幂等性校验） | `BannerConsumer` |
| **mq/producer** | Kafka生产者，发送消息 | `BannerMqProducer` |
| **lock** | 分布式锁，保证并发安全（UUID+Lua脚本） | `RedisDistributedLock` |
| **scheduler** | 定时任务，数据兜底刷新（基于XXL-Job） | `BannerScheduledTask` |
| **convert** | 对象转换器，Entity ↔ DTO | `BannerConvert` |
| **common/config** | 配置类（全局异常处理） | `GlobalExceptionHandler` |
| **common/util** | 工具类（JSON序列化、XSS过滤） | `JsonUtil`, `XssUtil`, `StringUtils`, `DateUtil` |
| **common/dto** | 数据传输对象 | `BannerInfo`, `BannerQueryRequest`, `ApiResponse` |

### 2.3 架构分层详解

#### 第一层：接入层

| 组件 | 职责 | 关键技术 |
|------|------|----------|
| **API Gateway** | 参数校验、输入清洗 | Spring Validation |
| **BannersController** | HTTP接口入口、请求分发 | @RestController, @Valid |

#### 第二层：业务逻辑层

| 组件 | 职责 | 关键技术 |
|------|------|----------|
| **BannerService** | 业务逻辑编排 | Service层 |
| **BannerServiceImpl** | 具体业务实现（分布式锁、日期计算） | UUID锁值、Lua脚本 |
| **BannerCacheManager** | 缓存读写管理（版本号校验、XSS过滤） | Redis操作封装 |

#### 第三层：数据访问层

| 组件 | 职责 | 关键技术 |
|------|------|----------|
| **BannerRepository** | Banner数据持久化 | JPA |
| **MessageProcessRecordRepository** | 幂等性记录管理 | JPA |

#### 第四层：基础设施层

| 组件 | 职责 | 关键技术 |
|------|------|----------|
| **BannerConsumer** | Kafka消息消费（幂等性校验） | @KafkaListener |
| **BannerMqProducer** | Kafka消息发送 | KafkaTemplate |
| **RedisDistributedLock** | 分布式锁（安全释放） | Lua脚本 |
| **XssUtil** | XSS防护 | HTML转义、URL协议白名单 |
| **JsonUtil** | JSON序列化（日期格式支持） | JavaTimeModule |
| **GlobalExceptionHandler** | 全局异常处理 | @RestControllerAdvice |

#### 第五层：数据存储层

| 存储 | 用途 | 数据结构 |
|------|------|----------|
| **MySQL** | Banner主数据、幂等性记录 | 关系型表 |
| **Redis** | Banner缓存、分布式锁 | Hash、String |
| **Kafka** | 消息队列 | Topic |

### 2.4 安全防护体系

| 防护点 | 位置 | 实现方式 |
|--------|------|----------|
| **参数校验** | Controller层 | @Valid + @NotBlank + @Pattern |
| **XSS过滤** | Service层 | HTML转义 + URL协议白名单（http/https） |
| **分布式锁安全** | Lock层 | UUID锁值 + Lua脚本校验归属后释放 |
| **幂等性** | Consumer层 | bannerId + version去重 |
| **版本号校验** | Cache层 | 防止旧数据覆盖新数据 |
| **全局异常处理** | Config层 | GlobalExceptionHandler |

### 2.5 技术选型

| 分类 | 技术 | 版本 | 说明 |
|------|------|------|------|
| 语言 | Java | 17 | LTS 版本，性能稳定 |
| 框架 | Spring Boot | 3.2.0 | 社区成熟，生态完善 |
| 数据库 | MySQL | 8.x | 关系型数据库，存储Banner数据 |
| 缓存 | Redis | 7.x | 高性能缓存，存储Banner查询数据 |
| 消息队列 | Kafka | 3.x | 异步消息传递，解耦运营端和消费端 |
| 定时任务 | XXL-Job | 2.4.0 | 分布式任务调度，支持动态配置和监控 |
| ORM | Spring Data JPA | 3.2.x | 简化数据库操作 |
| 连接池 | HikariCP | 5.x | Spring Boot 默认连接池，高性能 |

---

## 三、核心流程图

### 3.1 实时同步流程（Kafka消息）

```
运营端发送Kafka消息
        ↓
BannerConsumer接收消息
        ↓
解析BannerInfo对象
        ↓
设置ChangeType (UPDATE/DELETE)
        ↓
调用BannerService.syncBannerFromKafka()
        ↓
计算日期范围 (startDay ~ endDay)
        ↓
遍历每个日期
        ├─ 获取分布式锁 (lock:banner:{productId}:{date})
        │       ├─ 成功 → 执行缓存操作
        │       └─ 失败 → 跳过当前日期
        ├─ DELETE → 调用BannerCacheManager.deleteBanner()
        └─ UPDATE → 调用BannerCacheManager.setBanner()
        ↓
释放分布式锁
        ↓
同步完成
```

**关键代码路径**：
- `BannerConsumer.consumeBannerUpdate()` / `consumeBannerDelete()`
- `BannerServiceImpl.syncBannerFromKafka()`
- `BannerCacheManagerImpl.setBanner()` / `deleteBanner()`

### 3.2 C端查询流程

```
C端发起HTTP请求
        ↓
BannersController.getBanners()
        ↓
校验参数 (productId必填, date可选)
        ↓
调用BannerService.getBannersByDate()
        ↓
调用BannerCacheManager.getBannersByDate()
        ↓
从Redis读取Hash (banner:{productId}:{date})
        ↓
遍历Banner列表
        ├─ 过滤状态为启用的Banner (status == 1)
        └─ 按优先级降序排序
        ↓
转换为BannerSimpleInfo列表
        ↓
封装ApiResponse返回
```

**关键代码路径**：
- `BannersController.getBanners()`
- `BannerServiceImpl.getBannersByDate()`
- `BannerCacheManagerImpl.getBannersByDate()`

### 3.3 定时任务刷新流程

```
XXL-Job调度中心触发任务
        ↓
BannerScheduledTask执行任务
        ├─ 增量刷新任务 (bannerIncrementalRefreshJob)
        └─ 全量一致性检查任务 (bannerFullConsistencyCheckJob)
        ↓
调用BannerService.refreshFromDatabase()
        ↓
从MySQL查询Banner数据
        ├─ 增量：查询最近10分钟更新的数据
        └─ 全量：查询所有启用状态的数据
        ↓
Entity转换为DTO (Banner → BannerInfo)
        ↓
调用BannerCacheManager.refreshBannersByDate()
        ↓
删除Redis旧缓存
        ↓
写入新缓存
        ↓
刷新完成
```

**关键代码路径**：
- `BannerScheduledTask.incrementalRefresh()` / `fullConsistencyCheck()`
- `BannerServiceImpl.refreshFromDatabase()`
- `BannerCacheManagerImpl.refreshBannersByDate()`

**XXL-Job任务配置**：

| 任务名称 | JobHandler | Cron表达式 | 说明 |
|----------|------------|------------|------|
| bannerIncrementalRefreshJob | `bannerIncrementalRefreshJob` | `0 0/5 * * * ?` | 增量刷新，每5分钟执行 |
| bannerFullConsistencyCheckJob | `bannerFullConsistencyCheckJob` | `0 0/30 * * * ?` | 全量一致性检查，每30分钟执行 |

### 3.4 分布式锁机制

```
尝试获取锁
        ↓
SETNX lock:banner:{productId}:{date}
        ├─ 成功 → 执行业务逻辑 → 释放锁
        │       ↓
        │   Lua脚本判断锁值 → DELETE锁
        │
        └─ 失败 → 重试 (最多5次)
                ↓
                指数退避等待 (100ms → 300ms → 900ms → ...)
                ↓
                重试超过次数 → 放弃
```

**锁配置**：

| 配置项 | 值 | 说明 |
|--------|-----|------|
| LOCK_TIMEOUT_SECONDS | 30 | 锁超时时间，防止死锁 |
| MAX_RETRY_ATTEMPTS | 5 | 最大重试次数 |
| INITIAL_RETRY_INTERVAL_MS | 100 | 初始重试间隔 |

---

## 四、对外HTTP接口

### 4.1 接口总览

| HTTP方法 | 路径 | 功能 | 认证 |
|----------|------|------|------|
| GET | `/api/banners` | 查询指定日期的Banner列表 | 否 |

### 4.2 查询Banner列表

**请求**：
```
GET /api/banners?productId=prod01&date=20260616
```

**参数**：

| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| productId | String | 是 | 业务线标识，如 prod01 |
| date | String | 否 | 查询日期，格式 YYYYMMDD，默认今天 |

**响应**：
```json
{
  "code": 200,
  "msg": "success",
  "data": {
    "productId": "prod01",
    "date": "20260616",
    "banners": [
      {
        "bannerId": "banner_001",
        "title": "618大促Banner",
        "imageUrl": "https://example.com/image.jpg",
        "linkUrl": "https://example.com/activity",
        "priority": 100
      },
      {
        "bannerId": "banner_002",
        "title": "新人专享",
        "imageUrl": "https://example.com/image2.jpg",
        "linkUrl": "https://example.com/new-user",
        "priority": 50
      }
    ]
  }
}
```

**响应字段说明**：

| 字段 | 类型 | 说明 |
|------|------|------|
| code | Integer | 状态码，200表示成功 |
| msg | String | 状态描述 |
| data.productId | String | 业务线标识 |
| data.date | String | 查询日期 |
| data.banners | Array | Banner列表 |
| data.banners[].bannerId | String | Banner唯一ID |
| data.banners[].title | String | Banner标题 |
| data.banners[].imageUrl | String | 图片URL |
| data.banners[].linkUrl | String | 跳转链接 |
| data.banners[].priority | Integer | 优先级，数值越大越靠前 |

---

## 五、Kafka消息设计

### 5.1 消息Topic

| Topic名称 | 用途 | 消息格式 |
|-----------|------|----------|
| `banner-topic` | Banner变更消息（更新/删除） | JSON |

### 5.2 消息格式

**BannerInfo JSON格式**：
```json
{
  "bannerId": "banner_001",
  "productId": "prod01",
  "title": "618大促Banner",
  "imageUrl": "https://example.com/image.jpg",
  "linkUrl": "https://example.com/activity",
  "priority": 100,
  "status": 1,
  "startDay": "20260601",
  "endDay": "20260630",
  "updateTime": 1718563200000,
  "version": 1,
  "changeType": "UPDATE"
}
```

**字段说明**：

| 字段 | 类型 | 说明 |
|------|------|------|
| bannerId | String | Banner唯一业务ID |
| productId | String | 业务线标识 |
| title | String | 标题（会经过HTML转义） |
| imageUrl | String | 图片地址（仅支持http/https协议） |
| linkUrl | String | 跳转链接（仅支持http/https协议，禁止javascript） |
| priority | Integer | 优先级 |
| status | Integer | 状态：0-禁用，1-启用 |
| startDay | String | 生效开始日期 (YYYYMMDD) |
| endDay | String | 生效结束日期 (YYYYMMDD) |
| updateTime | Long | 更新时间戳 (毫秒) |
| version | Long | 版本号（严格递增，用于幂等性校验和版本控制） |
| changeType | String | 操作类型：`UPDATE`-更新，`DELETE`-删除 |

### 5.3 操作类型说明

| 操作类型 | 说明 | 触发场景 |
|----------|------|----------|
| `UPDATE` | 更新Banner缓存 | 新增Banner、修改Banner信息 |
| `DELETE` | 删除Banner缓存 | 删除Banner |

### 5.4 消费者组

| 配置项 | 值 | 说明 |
|--------|-----|------|
| group-id | `banner-consumer-group` | 消费者组ID |
| auto-offset-reset | `earliest` | 从头开始消费 |
| enable-auto-commit | `true` | 自动提交offset |

---

## 六、Redis存储设计

### 6.1 数据结构

| 数据类型 | Key格式 | Field | Value | 说明 |
|----------|---------|-------|-------|------|
| Hash | `banner:{productId}:{date}` | bannerId | BannerInfo JSON | 存储指定日期的Banner列表 |

**示例**：
```
Key: banner:prod01:20260616
Field: banner_001
Value: {"bannerId":"banner_001","title":"618大促",...}

Field: banner_002
Value: {"bannerId":"banner_002","title":"新人专享",...}
```

### 6.2 分布式锁存储

| 数据类型 | Key格式 | Value | 说明 |
|----------|---------|-------|------|
| String | `lock:banner:{productId}:{date}` | UUID | 分布式锁，30秒过期 |

---

## 七、数据库设计

### 7.1 Banner表结构

| 字段名 | 类型 | 约束 | 说明 |
|--------|------|------|------|
| id | BIGINT | PRIMARY KEY, AUTO_INCREMENT | 自增主键 |
| banner_id | VARCHAR(64) | NOT NULL, UNIQUE | Banner唯一业务ID |
| product_id | VARCHAR(32) | NOT NULL | 业务线标识 |
| title | VARCHAR(128) | NOT NULL | 标题 |
| image_url | VARCHAR(512) | NOT NULL | 图片地址 |
| link_url | VARCHAR(512) | NULL | 跳转链接 |
| priority | INT | NOT NULL, DEFAULT 0 | 优先级 |
| status | TINYINT | NOT NULL, DEFAULT 1 | 状态：0-禁用，1-启用 |
| start_day | DATE | NOT NULL | 生效开始日期 |
| end_day | DATE | NOT NULL | 生效结束日期 |
| create_time | DATETIME | NOT NULL, DEFAULT CURRENT_TIMESTAMP | 创建时间 |
| update_time | DATETIME | NOT NULL, ON UPDATE CURRENT_TIMESTAMP | 更新时间 |

**索引设计**：

| 索引名 | 字段 | 类型 |
|--------|------|------|
| uk_banner_id | banner_id | UNIQUE |
| idx_product_date | product_id, start_day, end_day | NORMAL |
| idx_update_time | update_time | NORMAL |

### 7.2 消息处理记录表结构

用于幂等性校验，记录已处理的消息版本号。

| 字段名 | 类型 | 约束 | 说明 |
|--------|------|------|------|
| id | BIGINT | PRIMARY KEY, AUTO_INCREMENT | 自增主键 |
| banner_id | VARCHAR(64) | NOT NULL | Banner唯一业务ID |
| version | BIGINT | NOT NULL | 消息版本号 |
| create_time | DATETIME | NOT NULL, DEFAULT CURRENT_TIMESTAMP | 创建时间 |

**索引设计**：

| 索引名 | 字段 | 类型 |
|--------|------|------|
| idx_banner_id | banner_id | NORMAL |
| idx_create_time | create_time | NORMAL |

---

## 八、配置说明

### 8.1 配置文件结构

```yaml
server:
  port: 8080                    # 服务端口

spring:
  application:
    name: banner-consumer-system
  datasource:                    # 数据库配置
    url: jdbc:mysql://localhost:3306/banner_db?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai
    username: root
    password: password
    driver-class-name: com.mysql.cj.jdbc.Driver
  jpa:                          # JPA配置
    hibernate:
      ddl-auto: update
    show-sql: false
    properties:
      hibernate:
        dialect: org.hibernate.dialect.MySQLDialect
  data:
    redis:                      # Redis配置
      host: localhost
      port: 6379
      database: 0
  kafka:                        # Kafka配置
    bootstrap-servers: localhost:9092
    consumer:
      group-id: banner-consumer-group
      auto-offset-reset: earliest
      enable-auto-commit: true
      key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
      value-deserializer: org.apache.kafka.common.serialization.StringDeserializer

banner:                         # 业务配置
  lock-timeout-seconds: 30      # 分布式锁超时时间
  max-retry-attempts: 5         # 锁重试次数
  initial-retry-interval-ms: 100 # 初始重试间隔
  incremental-window-minutes: 10 # 增量刷新时间窗口

xxl:                            # XXL-Job配置
  job:
    admin:
      addresses: http://localhost:8080/xxl-job-admin  # XXL-Job调度中心地址
    accessToken: default_token                           # 访问令牌
    executor:
      appname: banner-consumer                           # 执行器名称
      address:                                           # 执行器地址（可选）
      ip:                                                # 执行器IP（可选）
      port: 9999                                         # 执行器端口
      logpath: /data/applogs/xxl-job/jobhandler          # 日志路径
      logretentiondays: 30                               # 日志保留天数

logging:                        # 日志配置
  level:
    com.banner: INFO
    org.springframework.kafka: INFO
```

### 8.2 核心配置项说明

| 配置项 | 默认值 | 说明 |
|--------|--------|------|
| server.port | 8080 | 服务端口 |
| spring.datasource.url | jdbc:mysql://localhost:3306/banner_db | MySQL连接地址 |
| spring.datasource.username | root | 数据库用户名 |
| spring.datasource.password | password | 数据库密码 |
| spring.data.redis.host | localhost | Redis主机 |
| spring.data.redis.port | 6379 | Redis端口 |
| spring.kafka.bootstrap-servers | localhost:9092 | Kafka集群地址 |
| spring.kafka.consumer.group-id | banner-consumer-group | 消费者组ID |
| banner.lock-timeout-seconds | 30 | 分布式锁超时时间(秒) |
| banner.max-retry-attempts | 5 | 锁获取最大重试次数 |
| banner.incremental-window-minutes | 10 | 增量刷新时间窗口(分钟) |
| xxl.job.admin.addresses | http://localhost:8080/xxl-job-admin | XXL-Job调度中心地址 |
| xxl.job.accessToken | default_token | XXL-Job访问令牌 |
| xxl.job.executor.appname | banner-consumer | 执行器名称 |
| xxl.job.executor.port | 9999 | 执行器端口 |

### 8.3 常量配置

在 `BannerConstants.java` 中定义的常量：

| 常量 | 值 | 说明 |
|------|-----|------|
| BANNER_CACHE_KEY_PREFIX | `banner:` | Redis缓存Key前缀 |
| LOCK_KEY_PREFIX | `lock:banner:` | 分布式锁Key前缀 |
| LOCK_TIMEOUT_SECONDS | 30 | 锁超时时间 |
| MAX_RETRY_ATTEMPTS | 5 | 最大重试次数 |
| INITIAL_RETRY_INTERVAL_MS | 100 | 初始重试间隔(毫秒) |
| INCREMENTAL_WINDOW_MINUTES | 10 | 增量刷新窗口(分钟) |
| KAFKA_TOPIC_BANNER | `banner-topic` | Banner变更消息Topic（更新/删除） |
| KAFKA_CONSUMER_GROUP_ID | `banner-consumer-group` | 消费者组ID |
| DATE_FORMAT | `yyyyMMdd` | 日期格式 |

---

## 九、部署与运行

### 9.1 环境依赖

| 依赖 | 版本要求 | 用途 |
|------|----------|------|
| JDK | 17+ | Java运行环境 |
| MySQL | 8.0+ | 数据库 |
| Redis | 7.0+ | 缓存 |
| Kafka | 3.0+ | 消息队列 |
| XXL-Job | 2.4.0+ | 分布式任务调度 |

### 9.2 XXL-Job部署说明

#### 9.2.1 部署XXL-Job调度中心

1. 下载XXL-Job源码：`https://github.com/xuxueli/xxl-job`
2. 执行数据库脚本：`/xxl-job/doc/db/tables_xxl_job.sql`
3. 修改配置文件：`/xxl-job/xxl-job-admin/src/main/resources/application.properties`
4. 编译打包：`mvn clean package`
5. 启动调度中心：`java -jar xxl-job-admin-2.4.0.jar`

#### 9.2.2 配置执行器

在XXL-Job调度中心配置执行器：

| 配置项 | 值 |
|--------|-----|
| 执行器名称 | banner-consumer |
| 注册方式 | 自动注册 |
| 机器地址 | 自动注册（端口9999） |

#### 9.2.3 创建任务

在XXL-Job调度中心创建以下任务：

| 任务名称 | JobHandler | Cron表达式 | 说明 |
|----------|------------|------------|------|
| Banner增量刷新 | bannerIncrementalRefreshJob | 0 0/5 * * * ? | 增量刷新，每5分钟执行 |
| Banner全量一致性检查 | bannerFullConsistencyCheckJob | 0 0/30 * * * ? | 全量一致性检查，每30分钟执行 |

### 9.3 启动命令

```bash
# 开发环境
mvn spring-boot:run

# 生产环境
mvn clean package
java -jar target/banner-consumer-system-1.0.0.jar
```

### 9.4 配置文件切换

```bash
# 使用不同环境配置
java -jar target/banner-consumer-system-1.0.0.jar --spring.profiles.active=prod
```

### 9.5 启动顺序

1. 启动MySQL、Redis、Kafka
2. 启动XXL-Job调度中心
3. 启动Banner消费端系统
4. 在XXL-Job调度中心配置执行器和任务

---

## 十、代码目录结构

```
banner-consumer-system/
├── src/main/java/com/banner/
│   │
│   ├── BannerConsumerApplication.java    # 启动类
│   │
│   ├── common/                           # 公共模块
│   │   ├── constant/                     # 常量定义
│   │   │   └── BannerConstants.java
│   │   ├── dto/                          # 数据传输对象
│   │   │   ├── ApiResponse.java
│   │   │   ├── BannerInfo.java
│   │   │   ├── BannerListResponse.java
│   │   │   └── BannerQueryRequest.java
│   │   ├── entity/                       # 数据库实体
│   │   │   └── Banner.java
│   │   ├── util/                         # 工具类
│   │   │   ├── DateUtil.java
│   │   │   ├── JsonUtil.java
│   │   │   ├── StringUtils.java
│   │   │   └── XssUtil.java
│   │   └── config/                       # 配置类
│   │       ├── GlobalExceptionHandler.java
│   │       ├── KafkaConfig.java
│   │       ├── RedisConfig.java
│   │       └── XxlJobConfig.java
│   │
│   ├── controller/                       # 控制层
│   │   └── BannersController.java
│   │
│   ├── service/                          # 业务逻辑层
│   │   ├── BannerService.java            # 接口
│   │   ├── BannerCacheManager.java       # 接口
│   │   └── impl/                         # 实现类
│   │       ├── BannerServiceImpl.java
│   │       └── BannerCacheManagerImpl.java
│   │
│   ├── repository/                       # 数据访问层
│   │   ├── BannerRepository.java
│   │   ├── MessageProcessRecord.java
│   │   └── MessageProcessRecordRepository.java
│   │
│   ├── convert/                          # 对象转换器
│   │   └── BannerConvert.java
│   │
│   ├── mq/                               # 消息队列
│   │   ├── consumer/                     # 消费者
│   │   │   └── BannerConsumer.java
│   │   └── producer/                     # 生产者
│   │       └── BannerMqProducer.java
│   │
│   ├── lock/                             # 分布式锁
│   │   └── RedisDistributedLock.java
│   │
│   └── scheduler/                        # 定时任务（XXL-Job）
│       └── BannerScheduledTask.java
│
├── src/main/resources/
│   ├── application.yml                   # 应用配置
│   └── banner-schema.sql                 # 数据库初始化脚本
│
├── docs/                                 # 文档
│   ├── README.md
│   └── tech-design.md                    # 技术方案文档
│
└── pom.xml                               # Maven配置
```
