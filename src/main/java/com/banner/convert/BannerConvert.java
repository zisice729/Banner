package com.banner.convert;

import com.banner.common.dto.SimpleBannerInfo;
import com.banner.common.entity.SimpleBanner;
import org.springframework.stereotype.Component;

@Component
public class BannerConvert {

    public SimpleBannerInfo toSimpleBannerInfo(SimpleBanner banner) {
        if (banner == null) {
            return null;
        }

        SimpleBannerInfo info = new SimpleBannerInfo();
        info.setBannerId(banner.getBannerId());
        info.setProductId(banner.getProductId());
        info.setTitle(banner.getTitle());
        info.setImageUrl(banner.getImageUrl());
        info.setLinkUrl(banner.getLinkUrl());
        info.setPriority(banner.getPriority());
        info.setStatus(banner.getStatus());
        info.setStartDay(banner.getStartDay() != null ? 
                banner.getStartDay().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd")) : null);
        info.setEndDay(banner.getEndDay() != null ? 
                banner.getEndDay().format(java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd")) : null);
        info.setVersion(banner.getUpdateTime() != null ? 
                banner.getUpdateTime().atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli() : null);
        return info;
    }
}
