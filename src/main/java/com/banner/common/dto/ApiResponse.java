package com.banner.common.dto;

/**
 * 统一API响应封装
 * 包含业务状态码、消息和数据
 */
public class ApiResponse<T> {

    /**
     * 业务状态码：200-成功，其他-失败
     */
    private Integer code;

    /**
     * 响应消息
     */
    private String message;

    /**
     * 响应数据
     */
    private T data;

    public ApiResponse() {
    }

    public ApiResponse(Integer code, String message, T data) {
        this.code = code;
        this.message = message;
        this.data = data;
    }

    /**
     * 成功响应（无数据）
     *
     * @return ApiResponse实例
     */
    public static ApiResponse<Void> success() {
        return new ApiResponse<>(200, "success", null);
    }

    /**
     * 成功响应（带数据）
     *
     * @param data 响应数据
     * @param <T>  数据类型
     * @return ApiResponse实例
     */
    public static <T> ApiResponse<T> success(T data) {
        return new ApiResponse<>(200, "success", data);
    }

    /**
     * 失败响应
     *
     * @param message 失败消息
     * @return ApiResponse实例
     */
    public static ApiResponse<Void> fail(String message) {
        return new ApiResponse<>(500, message, null);
    }

    /**
     * 失败响应（带状态码）
     *
     * @param code    状态码
     * @param message 失败消息
     * @return ApiResponse实例
     */
    public static ApiResponse<Void> fail(Integer code, String message) {
        return new ApiResponse<>(code, message, null);
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}