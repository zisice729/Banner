package com.banner.common.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Size;

/**
 * Banner查询请求
 */
public class BannerQueryRequest {

    @NotBlank(message = "productId不能为空")
    @Size(max = 32, message = "productId长度不能超过32字符")
    @Pattern(regexp = "^[a-zA-Z0-9_\\-]+$", message = "productId只能包含字母、数字、下划线和连字符")
    private String productId;

    @Pattern(regexp = "^\\d{8}$", message = "date格式必须为YYYYMMDD")
    private String date;

    public BannerQueryRequest() {
    }

    public String getProductId() {
        return productId;
    }

    public void setProductId(String productId) {
        this.productId = productId;
    }

    public String getDate() {
        return date;
    }

    public void setDate(String date) {
        this.date = date;
    }
}
