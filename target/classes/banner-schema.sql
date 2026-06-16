CREATE DATABASE IF NOT EXISTS banner_db DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

USE banner_db;

CREATE TABLE IF NOT EXISTS `banner` (
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

-- 插入测试数据
INSERT INTO `banner` (`banner_id`, `product_id`, `title`, `image_url`, `link_url`, `priority`, `status`, `start_day`, `end_day`) VALUES
('banner_001', 'prod01', '618大促Banner', 'https://example.com/image1.jpg', 'https://example.com/activity', 100, 1, '2026-05-25', '2026-06-05'),
('banner_002', 'prod01', '新用户专享', 'https://example.com/image2.jpg', 'https://example.com/newuser', 90, 1, '2026-05-25', '2026-06-30'),
('banner_003', 'prod02', '会员福利', 'https://example.com/image3.jpg', 'https://example.com/vip', 80, 1, '2026-05-25', '2026-12-31');
