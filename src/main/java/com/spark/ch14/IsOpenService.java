package com.spark.ch14;

import java.sql.Timestamp;
import java.util.Calendar;

public class IsOpenService {
    public static Boolean isOpen(String hoursMon, String hoursTue, String hoursWed, String hoursThu, String hoursFri, String hoursSat, String hoursSun, Timestamp dateTime) {

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(dateTime.getTime());
        int day = calendar.get(Calendar.DAY_OF_WEEK);
        String hours;
        switch (day) {
            case Calendar.MONDAY -> hours = hoursMon;
            case Calendar.TUESDAY -> hours = hoursTue;
            case Calendar.WEDNESDAY -> hours = hoursWed;
            case Calendar.THURSDAY -> hours = hoursThu;
            case Calendar.FRIDAY -> hours = hoursFri;
            case Calendar.SATURDAY -> hours = hoursSat;
            default -> hours = hoursSun;
        }

        if (hours.compareToIgnoreCase("closed") == 0) {
            return false;
        }

        int event = calendar.get(Calendar.HOUR_OF_DAY) * 3600 + calendar.get(Calendar.MINUTE) * 60 + calendar.get(Calendar.SECOND);

        String[] ranges = hours.split(" and ");
        for (String range : ranges) {
            String[] openingHours = range.split("-");
            int start = Integer.parseInt(openingHours[0].substring(0, 2)) * 3600 + Integer.parseInt(openingHours[0].substring(3, 5)) * 60;
            int end = Integer.parseInt(openingHours[1].substring(0, 2)) * 3600 + Integer.parseInt(openingHours[1].substring(3, 5)) * 60;

            if (event >= start && event <= end) {
                return true;
            }
        }

        return false;
    }
}
