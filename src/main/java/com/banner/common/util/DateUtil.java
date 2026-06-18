package com.banner.common.util;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.time.ZoneId;
import java.util.List;

public class DateUtil {

    private static final DateTimeFormatter FORMATTER_YYYYMMDD = DateTimeFormatter.ofPattern("yyyyMMdd");

    private DateUtil() {
    }

    public static String todayStr() {
        return LocalDate.now().format(FORMATTER_YYYYMMDD);
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
