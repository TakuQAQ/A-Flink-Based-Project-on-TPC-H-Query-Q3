package com.taku.tpch.utils;

public class DateParser {
    public static long parseDate(String dateStr) {
        return java.time.LocalDate.parse(dateStr)
                .atStartOfDay(java.time.ZoneId.systemDefault())
                .toInstant()
                .toEpochMilli();
    }
}