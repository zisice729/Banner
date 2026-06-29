package com.banner.common.enums;

/**
 * Kafka消息类型枚举
 * 用于区分Banner数据变更消息的操作类型
 */
public enum MessageType {

    /**
     * 更新/创建操作
     */
    UPDATE(1),

    /**
     * 删除操作（软删除）
     */
    DELETE(2);

    /**
     * 消息类型编码
     */
    private final Integer code;

    MessageType(Integer code) {
        this.code = code;
    }

    /**
     * 获取消息类型编码
     *
     * @return 类型编码
     */
    public Integer getCode() {
        return code;
    }
}