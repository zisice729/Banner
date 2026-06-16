package com.banner.repository;

import jakarta.persistence.*;

import java.time.LocalDateTime;

/**
 * 消息处理记录
 */
@Entity
@Table(name = "message_process_record")
public class MessageProcessRecord {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "banner_id", nullable = false, length = 64)
    private String bannerId;

    @Column(name = "version", nullable = false)
    private Long version;

    @Column(name = "create_time", nullable = false)
    private LocalDateTime createTime;

    public MessageProcessRecord() {
    }

    public MessageProcessRecord(String bannerId, Long version) {
        this.bannerId = bannerId;
        this.version = version;
        this.createTime = LocalDateTime.now();
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getBannerId() {
        return bannerId;
    }

    public void setBannerId(String bannerId) {
        this.bannerId = bannerId;
    }

    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    public LocalDateTime getCreateTime() {
        return createTime;
    }

    public void setCreateTime(LocalDateTime createTime) {
        this.createTime = createTime;
    }
}
