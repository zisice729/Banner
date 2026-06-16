# Banner消费端系统完整设计文档

## 一、项目概述

### 1.1 项目定位

Banner消费端系统是一个**高性能的Banner数据同步与查询服务**，负责：
- 接收其他管理系统通过Kafka推送的Banner数据并同步到Redis
- 通过定时任务连接MySQL进行数据兜底刷新，确保最终一致性
- 为C端提供高性能的Banner查询接口

### 1.2 核心特点

- **高性能读**：C端直接读取Redis，毫秒级响应
- **高可靠性**：双路径数据同步（Kafka实时同步 + 定时任务兜底）
- **最终一致性**：多级保障机制确保数据正确
- **代码规范**：分层清晰，Service层采用接口+实现类
- **模块化设计**：common模块统一管理公共组件

---

## 二、架构设计

### 2.1 整体架构图

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Banner消费端系统                              │
│                                                                      │
│  ┌──────────────────┐  ┌──────────────────┐  ┌─────────────────┐ │
│  │  C端查询API      │  │  Kafka消费者      │  │  定时任务层     │ │
│  │  (Controller)   │  │  (mq/consumer)   │  │  (scheduler)    │ │
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬────────┘ │
│           │                     │                    │            │
│           ▼                     ▼                    ▼            │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │                    业务逻辑层                                ││
│  │  ┌─────────────────┐  ┌─────────────────┐                  ││
│  │  │ BannerService  │  │ BannerCacheManager             │  ││
│  │  └─────────────────┘  └─────────────────┘                  ││
│  └──────────────────┬───────────────────┬──────────────────┘│
│                     │                   │                   │
│                     ▼                   ▼                   │
│  ┌─────────────────────────────────────────────────────────────┐│
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐   ││
│  │  │ Repository │  │   Lock      │  │   Convert       │   ││
│  │  │ (持久化)    │  │ (分布式锁)  │  │ (实体转换)      │   ││
│  │  └─────────────┘  └─────────────┘  └─────────────────┘   ││
│  └──────────────────────────────────────────────────────────┘│
└────────────────────────────────────────────────────────────────┘

                              ↓

┌──────────────────────────────────────────────────────────────────┐
│                        common公共模块                           │
│  ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐  │
│  │   config   │ │    util    │ │  constant  │ │    dto     │  │
│  │ (配置类)   │ │  (工具类)  │ │   (常量)   │ │  (DTO)    │  │
│  └────────────┘ └────────────┘ └────────────┘ └────────────┘  │
│  ┌────────────┐                                                │
│  │   entity   │                                                │
│  │  (实体)    │                                                │
│  └────────────┘                                                │
└──────────────────────────────────────────────────────────────────┘

                              ↓

┌──────────────────────────────────────────────────────────────────┐
│                        外部依赖层                               │
│  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐  │
│  │   Redis         │ │  MySQL (管理端) │ │  Kafka Broker    │  │
│  │  - Banner缓存   │ │  - banner表     │ │  - banner-topic │  │
│  │  - 分布式锁     │ │  - 幂等性记录    │ │  (UPDATE/DELETE) │  │
│  └─────────────────┘ └─────────────────┘ └─────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

### 2.2 核心模块职责

| 模块 | 职责 |
|------|------|
| **common** | 公共模块：配置、工具类、常量、DTO、实体 |
| **controller** | 控制层：C端查询接口 |
| **service** | 业务逻辑层：BannerService、BannerCacheManager（接口+实现） |
| **repository** | 数据访问层：数据库操作 |
| **convert** | 转换器：Entity ↔ DTO |
| **mq** | 消息队列：producer（生产者）+ consumer（消费者） |
| **scheduler** | 定时任务：增量刷新 + 全量一致性检查 |
| **lock** | 分布式锁：Redis分布式锁实现 |

---

## 三、项目文件目录

### 3.1 完整目录结构

```
banner-consumer-system/
├── src/main/java/com/banner/
│   │
│   ├── BannerConsumerApplication.java       # 启动类
│   │
│   ├── common/                            # 公共模块
│   │   ├── config/                        # 配置类
│   │   │   ├── KafkaConfig.java          # Kafka配置
│   │   │   └── RedisConfig.java         # Redis配置
│   │   ├── util/                          # 工具类
│   │   │   ├── DateUtil.java             # 日期工具
│   │   │   └── JsonUtil.java             # JSON工具
│   │   ├── constant/                      # 常量定义
│   │   │   └── BannerConstants.java     # Banner常量
│   │   ├── dto/                          # 数据传输对象
│   │   │   ├── ApiResponse.java          # 统一响应
│   │   │   ├── BannerInfo.java           # Banner信息DTO
│   │   │   └── BannerListResponse.java  # Banner列表响应
│   │   └── entity/                       # 数据库实体
│   │       └── Banner.java               # Banner实体
│   │
│   ├── controller/                        # 控制层
│   │   └── BannersController.java        # C端查询接口
│   │
│   ├── service/                          # 业务逻辑层
│   │   ├── BannerService.java           # Banner业务逻辑接口
│   │   ├── BannerCacheManager.java     # 缓存管理接口
│   │   └── impl/                        # 实现类
│   │       ├── BannerServiceImpl.java   # Banner业务逻辑实现
│   │       └── BannerCacheManagerImpl.java # 缓存管理实现
│   │
│   ├── repository/                      # 数据访问层
│   │   └── BannerRepository.java       # Banner数据访问
│   │
│   ├── convert/                        # 转换器
│   │   └── BannerConvert.java         # Banner实体转换器
│   │
│   ├── mq/                            # 消息队列
│   │   ├── producer/                  # 生产者
│   │   │   └── BannerMqProducer.java # Banner消息生产者
│   │   └── consumer/                  # 消费者
│   │       └── BannerConsumer.java   # Banner消息消费者
│   │
│   ├── scheduler/                    # 定时任务
│   │   └── BannerScheduledTask.java # 定时刷新任务
│   │
│   └── lock/                        # 分布式锁
│       └── RedisDistributedLock.java # Redis分布式锁
│
├── src/main/resources/
│   ├── application.yml              # 应用配置
│   └── banner-schema.sql            # 数据库脚本
│
└── pom.xml                           # Maven配置
```

### 3.2 文件说明

| 目录 | 文件 | 说明 |
|------|------|------|
| **common/config** | | |
| | KafkaConfig.java | Kafka消费者配置 |
| | RedisConfig.java | Redis模板配置 |
| **common/util** | | |
| | DateUtil.java | 日期转换工具 |
| | JsonUtil.java | JSON序列化工具 |
| **common/constant** | | |
| | BannerConstants.java | 系统常量定义 |
| **common/dto** | | |
| | ApiResponse.java | 统一响应格式 |
| | BannerInfo.java | Banner数据传输对象 |
| | BannerListResponse.java | Banner列表响应 |
| **common/entity** | | |
| | Banner.java | Banner数据库实体 |
| **controller** | | |
| | BannersController.java | C端查询API |
| **service** | | |
| | BannerService.java | 业务逻辑接口 |
| | BannerCacheManager.java | 缓存管理接口 |
| | impl/BannerServiceImpl.java | 业务逻辑实现 |
| | impl/BannerCacheManagerImpl.java | 缓存管理实现 |
| **repository** | | |
| | BannerRepository.java | 数据访问接口 |
| **convert** | | |
| | BannerConvert.java | 实体转换器 |
| **mq/producer** | | |
| | BannerMqProducer.java | 消息生产者 |
| **mq/consumer** | | |
| | BannerConsumer.java | 消息消费者 |
| **scheduler** | | |
| | BannerScheduledTask.java | 定时任务 |
| **lock** | | |
| | RedisDistributedLock.java | 分布式锁 |

---

## 四、数据库设计

### 4.1 Banner配置表

```sql
CREATE TABLE `banner` (
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
```

---

## 五、Redis存储设计

### 5.1 Banner缓存设计

| 项目 | 设计 |
|------|------|
| **Key格式** | `banner:{productId}:{date}` |
| **数据结构** | Hash |
| **Field** | `bannerId` |
| **Value** | BannerInfo JSON字符串 |
| **示例** | `banner:prod01:20260526` |

### 5.2 分布式锁设计

| 项目 | 设计 |
|------|------|
| **Key格式** | `lock:banner:{productId}:{date}` |
| **锁类型** | Redis SET NX EX |
| **超时时间** | 30秒 |
| **粒度** | 按(productId, date)加锁 |

---

## 六、Kafka消息设计

### 6.1 消息主题

| 主题 | 说明 |
|------|------|
| `banner-topic` | Banner变更消息（更新/删除），通过changeType区分操作类型 |

### 6.2 消息体结构

```json
{
  "bannerId": "banner_001",
  "productId": "prod01",
  "title": "618大促Banner",
  "imageUrl": "https://example.com/image.jpg",
  "linkUrl": "https://example.com/activity",
  "priority": 100,
  "status": 1,
  "startDay": "2026-05-25",
  "endDay": "2026-06-05",
  "updateTime": 1716700800000,
  "changeType": "UPDATE"
}
```

### 6.3 操作类型说明

| changeType | 说明 | 触发场景 |
|------------|------|----------|
| `UPDATE` | 更新Banner缓存 | 新增Banner、修改Banner信息 |
| `DELETE` | 删除Banner缓存 | 删除Banner |

---

## 七、接口文档

### 7.1 C端查询接口

#### 获取指定日期的Banner列表

**请求**：
```
GET /api/banners?productId=prod01&date=20260526
```

**参数**：
| 参数 | 类型 | 必填 | 说明 |
|------|------|------|------|
| productId | String | 是 | 业务线标识 |
| date | String | 否 | 日期（YYYYMMDD），默认今天 |

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

---

## 八、核心功能设计

### 8.1 实时同步流程

```
Kafka推送Banner变更
    ↓
BannerConsumer接收消息
    ↓
计算受影响日期范围（startDay ~ endDay）
    ↓
对每个日期尝试获取锁（指数退避重试）
    ↓
┌─ 成功：比较updateTime，只更新最新数据 → 写入Redis → 释放锁
│
└─ 失败：重试5次后直接丢弃，不处理
```

### 8.2 定时任务流程

#### 增量刷新（每5分钟）

```
查询最近10分钟更新的Banner
    ↓
对每个Banner计算受影响日期
    ↓
尝试获取锁
    ├─ 成功：全量刷新该日期Redis数据
    └─ 失败：直接跳过，不记录
    ↓
释放锁
```

#### 全量一致性检查（每30分钟）

```
获取所有启用的Banner
    ↓
按productId和date分组
    ↓
尝试获取锁
    ├─ 成功：全量刷新，确保数据一致
    └─ 失败：直接跳过
    ↓
释放锁
```

---

## 九、最终一致性保障

### 9.1 保障机制

| 层级 | 机制 | 说明 |
|------|------|------|
| **第1层** | Kafka实时同步 + 指数退避重试 | 快速响应变更，最多重试5次 |
| **第2层** | 版本比较（updateTime） | 防止旧数据覆盖新数据 |
| **第3层** | 定时任务增量刷新（5分钟） | 数据兜底 |
| **第4层** | 定时任务全量检查（30分钟） | 最终一致性保障 |

### 9.2 失败处理策略

| 场景 | 处理策略 |
|------|----------|
| 实时同步获取锁失败 | 指数退避重试（100ms, 300ms, 900ms, 2700ms, 8100ms），5次后放弃 |
| 定时任务获取锁失败 | 直接跳过，不记录，不重试 |
| 其他异常 | 记录日志，放弃处理 |

---

## 十、技术栈

| 组件 | 选型 | 版本 |
|------|------|------|
| 开发框架 | Spring Boot | 3.2.0 |
| 数据库 | MySQL | 8.0 |
| 消息队列 | Kafka | 3.1.2 |
| 缓存 | Redis | 7.0 |
| ORM | Spring Data JPA | - |
| Java版本 | JDK | 17 |
| 构建工具 | Maven | - |

---

## 十一、关键配置说明

### application.yml

```yaml
server:
  port: 8080

spring:
  datasource:
    url: jdbc:mysql://localhost:3306/banner_db?useUnicode=true&characterEncoding=utf8&serverTimezone=Asia/Shanghai
    username: root
    password: password
    driver-class-name: com.mysql.cj.jdbc.Driver
  jpa:
    hibernate:
      ddl-auto: update
    show-sql: false
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

banner:
  lock-timeout-seconds: 30
  max-retry-attempts: 5
  initial-retry-interval-ms: 100
  incremental-window-minutes: 10

logging:
  level:
    com.banner: INFO
    org.springframework.kafka: INFO
```

---

## 十二、开发学习要点

这个项目适合学习的知识点：

1. **Kafka消息消费**：消息处理、异常处理
2. **Redis应用**：Hash数据结构、分布式锁、缓存管理
3. **定时任务**：Spring Scheduler、任务调度
4. **JPA数据访问**：Repository、查询优化
5. **分层架构**：接口+实现类分层设计
6. **模块化设计**：common公共模块抽取
7. **高可用设计**：重试机制、多路径保障、最终一致性
8. **工具类设计**：日期工具、JSON工具、常量管理
9. **实体转换**：Entity ↔ DTO 转换器设计
