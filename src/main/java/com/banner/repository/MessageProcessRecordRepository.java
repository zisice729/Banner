package com.banner.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;

/**
 * 消息处理记录Repository
 */
@Repository
public interface MessageProcessRecordRepository extends JpaRepository<MessageProcessRecord, Long> {

    @Query("SELECT r FROM MessageProcessRecord r WHERE r.bannerId = :bannerId ORDER BY r.version DESC LIMIT 1")
    MessageProcessRecord findLatestByBannerId(@Param("bannerId") String bannerId);

    @Modifying
    @Query("DELETE FROM MessageProcessRecord r WHERE r.createTime < :before")
    int deleteRecordsBefore(@Param("before") LocalDateTime before);
}
