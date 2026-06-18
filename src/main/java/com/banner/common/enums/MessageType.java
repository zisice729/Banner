package com.banner.common.enums;

public enum MessageType {

    UPDATE(1),
    DELETE(2);

    private final Integer code;

    MessageType(Integer code) {
        this.code = code;
    }

    public Integer getCode() {
        return code;
    }
}
