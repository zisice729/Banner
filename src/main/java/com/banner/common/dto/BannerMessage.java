package com.banner.common.dto;

/**
 * Kafka消息体DTO
 * 用于接收上游发送的Banner数据变更消息
 */
public class BannerMessage {

    /**
     * 消息ID（UUID），用于幂等性校验
     */
    private String messageId;

    /**
     * Banner ID
     */
    private Long id;

    /**
     * 消息类型：1-更新/创建，2-删除（参考MessageType枚举）
     */
    private Integer type;

    public BannerMessage() {
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Integer getType() {
        return type;
    }

    public void setType(Integer type) {
        this.type = type;
    }
}