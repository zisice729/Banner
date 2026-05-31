package com.banner.repository;

import com.banner.common.entity.SimpleBanner;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface SimpleBannerRepository extends JpaRepository<SimpleBanner, Long> {

    @Query("SELECT b FROM SimpleBanner b WHERE b.productId = :productId " +
           "AND b.startDay <= :date AND b.endDay >= :date")
    List<SimpleBanner> findByProductIdAndDate(
            @Param("productId") String productId,
            @Param("date") LocalDate date);

    @Query("SELECT b FROM SimpleBanner b WHERE b.updateTime >= :updateTime")
    List<SimpleBanner> findByUpdateTimeAfter(@Param("updateTime") LocalDateTime updateTime);

    List<SimpleBanner> findByStatus(Integer status);

    SimpleBanner findByBannerId(String bannerId);
}
