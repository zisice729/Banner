package com.banner.common.enums;

public enum MessageType {

    SYNC(1, "同步"),
    DELETE(2, "删除");

    private final int code;
    private final String desc;

    MessageType(int code, String desc) {
        this.code = code;
        this.desc = desc;
    }

    public int getCode() {
        return code;
    }

    public String getDesc() {
        return desc;
    }
}
