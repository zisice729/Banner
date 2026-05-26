package com.banner.convert;

import com.banner.common.dto.BannerInfo;
import com.banner.common.entity.Banner;
import org.springframework.stereotype.Component;

@Component
public class BannerConvert {

    public BannerInfo toBannerInfo(Banner banner) {
        if (banner == null) {
            return null;
        }

        BannerInfo info = new BannerInfo();
        info.setBannerId(banner.getBannerId());
        info.setProductId(banner.getProductId());
        info.setTitle(banner.getTitle());
        info.setImageUrl(banner.getImageUrl());
        info.setLinkUrl(banner.getLinkUrl());
        info.setPriority(banner.getPriority());
        info.setStatus(banner.getStatus());
        info.setStartDay(banner.getStartDay());
        info.setEndDay(banner.getEndDay());
        info.setUpdateTime(banner.getUpdateTime() != null ?
                banner.getUpdateTime().atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli() : null);
        return info;
    }

}
