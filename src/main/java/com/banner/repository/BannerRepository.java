package com.banner.repository;

import com.banner.common.entity.Banner;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface BannerRepository extends JpaRepository<Banner, Long> {

    @Query("SELECT b FROM Banner b WHERE b.productId = :productId " +
           "AND b.startDay <= :date AND b.endDay >= :date")
    List<Banner> findByProductIdAndDate(
            @Param("productId") String productId,
            @Param("date") LocalDate date);

    @Query("SELECT b FROM Banner b WHERE b.updateTime >= :updateTime")
    List<Banner> findByUpdateTimeAfter(@Param("updateTime") LocalDateTime updateTime);

    List<Banner> findByStatus(Integer status);

}
