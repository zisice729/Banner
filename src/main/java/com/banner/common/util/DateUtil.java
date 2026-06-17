package com.banner.common.util;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class DateUtil {

    private static final DateTimeFormatter FORMATTER_YYYYMMDD = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter FORMATTER_YYYY_MM_DD = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    public static String formatToYYYYMMDD(LocalDate date) {
        return date.format(FORMATTER_YYYYMMDD);
    }

    public static String formatToYYYYMMDD(Date date) {
        return date.toInstant().atZone(ZoneId.systemDefault()).toLocalDate().format(FORMATTER_YYYYMMDD);
    }

    public static String formatYYYYMMDD(Long timestamp) {
        if (timestamp == null) {
            return todayStr();
        }
        return new Date(timestamp).toInstant().atZone(ZoneId.systemDefault()).toLocalDate().format(FORMATTER_YYYYMMDD);
    }

    public static LocalDate parseYYYYMMDD(String dateStr) {
        return LocalDate.parse(dateStr, FORMATTER_YYYYMMDD);
    }

    public static List<String> getDateRange(LocalDate startDay, LocalDate endDay) {
        List<String> dates = new ArrayList<>();
        LocalDate current = startDay;
        while (!current.isAfter(endDay)) {
            dates.add(formatToYYYYMMDD(current));
            current = current.plusDays(1);
        }
        return dates;
    }

    public static String todayStr() {
        return LocalDate.now().format(FORMATTER_YYYYMMDD);
    }

    public static LocalDateTime minusMinutes(int minutes) {
        return LocalDateTime.now().minusMinutes(minutes);
    }

    public static Date toDate(LocalDateTime localDateTime) {
        return Date.from(localDateTime.atZone(ZoneId.systemDefault()).toInstant());
    }

}
