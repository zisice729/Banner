package com.banner.common.util;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.time.ZoneId;
import java.util.List;

/**
 * 日期工具类
 * 提供日期格式化、日期范围计算等功能
 */
public class DateUtil {

    /**
     * 日期格式化器：yyyyMMdd格式
     */
    private static final DateTimeFormatter FORMATTER_YYYYMMDD = DateTimeFormatter.ofPattern("yyyyMMdd");

    private DateUtil() {
    }

    /**
     * 获取今天日期字符串（格式：yyyyMMdd）
     *
     * @return 今天日期字符串
     */
    public static String todayStr() {
        return LocalDate.now().format(FORMATTER_YYYYMMDD);
    }

    /**
     * 获取当前日期字符串（格式：yyyyMMdd）
     * 与todayStr()功能相同，提供更直观的方法名
     *
     * @return 当前日期字符串
     */
    public static String getCurrentDate() {
        return LocalDate.now().format(FORMATTER_YYYYMMDD);
    }

    /**
     * 将Date对象格式化为yyyyMMdd格式字符串
     *
     * @param date Date对象
     * @return 格式化后的日期字符串
     */
    public static String formatToYYYYMMDD(Date date) {
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate().format(FORMATTER_YYYYMMDD);
    }

    /**
     * 将时间戳格式化为yyyyMMdd格式字符串
     *
     * @param timestamp 时间戳（毫秒）
     * @return 格式化后的日期字符串，timestamp为null返回今天日期
     */
    public static String formatYYYYMMDD(Long timestamp) {
        if (timestamp == null) {
            return todayStr();
        }
        return new Date(timestamp).toInstant().atZone(ZoneId.systemDefault()).toLocalDate().format(FORMATTER_YYYYMMDD);
    }

    /**
     * 计算日期范围
     * 获取从startTimestamp到endTimestamp之间的所有日期（包含起止日期）
     *
     * @param startTimestamp 开始时间戳（毫秒）
     * @param endTimestamp   结束时间戳（毫秒）
     * @return 日期字符串列表（格式：yyyyMMdd）
     */
    public static List<String> getDateRange(Long startTimestamp, Long endTimestamp) {
        List<String> dates = new ArrayList<>();
        if (startTimestamp == null || endTimestamp == null) {
            return dates;
        }

        LocalDate startDate = new Date(startTimestamp).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        LocalDate endDate = new Date(endTimestamp).toInstant().atZone(ZoneId.systemDefault()).toLocalDate();

        LocalDate current = startDate;
        while (!current.isAfter(endDate)) {
            dates.add(current.format(FORMATTER_YYYYMMDD));
            current = current.plusDays(1);
        }

        return dates;
    }
}