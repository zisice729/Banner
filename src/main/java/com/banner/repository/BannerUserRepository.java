package com.banner.repository;

import com.banner.common.entity.Banner;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface BannerUserRepository extends JpaRepository<Banner, Long> {

    @Query("SELECT b.userId FROM Banner b WHERE b.bannerId = :bannerId")
    List<Long> findUserIdsByBannerId(@Param("bannerId") String bannerId);

    void deleteByBannerId(String bannerId);

    long countByBannerId(String bannerId);
}
