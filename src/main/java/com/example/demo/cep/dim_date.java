package com.example.demo.cep;

/**
 * @author Applet
 * @version 1.0.0
 * @date 2023/12/18 11:39
 */


import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalField;
import java.time.temporal.WeekFields;

public class dim_date {
    public static void main(String[] args) {
        LocalDate startDate = LocalDate.of(2023, 1, 1);
        LocalDate endDate = LocalDate.of(2023, 12, 31);

        WeekFields weekFields = WeekFields.of(DayOfWeek.MONDAY, 1);

        //  System.out.printf("%-10s %-10s %-10s %-10s %-10s %-10s %-10s %-10s\n", "date_id", "week_id", "week_day", "day", "month", "quarter", "year", "is_workday");
        System.out.printf("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", "date_id", "week_id", "week_day", "day", "month", "quarter", "year", "is_workday");

        while (startDate.isBefore(endDate)) {
            String dateId = startDate.format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
            int weekOfYear = startDate.get(weekFields.weekOfYear());
            int weekDay = startDate.getDayOfWeek().getValue();
            int dayOfMonth = startDate.getDayOfMonth();
            int monthValue = startDate.getMonthValue();
            int quarterValue = (monthValue + 2) / 3;
            int year = startDate.getYear();
            boolean isWorkday = !startDate.getDayOfWeek().equals(DayOfWeek.SATURDAY) && !startDate.getDayOfWeek().equals(DayOfWeek.SUNDAY);
            String holidayId = "\\N";

            System.out.printf("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n", dateId, weekOfYear, weekDay, dayOfMonth, monthValue, quarterValue, year, isWorkday, holidayId);

            startDate = startDate.plusDays(1);
        }
    }
}


