package com.reinvent.synergy.data.system;

import org.apache.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.Days;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;

/**
 * @author Bohdan Mushkevych
 *         Description: holds logic common for time operations
 */
public class TimePeriodHelper {
    public static int toSynergyFormat(Date date, TimeQualifier qualifier) {
        DateFormat dateFormat = new SimpleDateFormat(qualifier.getPattern());
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        String formatted = dateFormat.format(date);
        return Integer.valueOf(formatted);
    }

    public static int incrementTimePeriod(int timePeriod, int by, TimeQualifier qualifier) {
        try {
            DateFormat dateFormat = new SimpleDateFormat(qualifier.getPattern().substring(0, qualifier.getMeaningfulPositions()));
            dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
            Date parsed = dateFormat.parse(String.valueOf(timePeriod).substring(0, qualifier.getMeaningfulPositions()));

            Calendar utcCalendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
            int field;
            utcCalendar.setTime(parsed);
            switch (qualifier) {
                case HOURLY:
                    field = Calendar.HOUR;
                    break;
                case DAILY:
                    field = Calendar.DAY_OF_YEAR;
                    break;
                case MONTHLY:
                    field = Calendar.MONTH;
                    break;
                case YEARLY:
                    field = Calendar.YEAR;
                    break;
                default:
                    throw new IllegalArgumentException("TimeQualifier " + qualifier.getRepresentation() + " is currently not supported");
            }
            utcCalendar.add(field, by);
            Date computed = utcCalendar.getTime();
            return TimePeriodHelper.toSynergyFormat(computed, qualifier);
        } catch (ParseException e) {
            Logger.getRootLogger().error(String.format("Can not increment time period %s.", timePeriod), e);
            return -1;
        }
    }

    /**
     * Convert a millisecond duration to a string format
     * author http://stackoverflow.com/a/7663966/1018977
     *
     * @param millis A duration to convert to a string form
     * @return A string of the form "X Days Y Hours Z Minutes A Seconds".
     */
    public static String getDurationBreakdown(long millis) {
        if (millis < 0) {
            throw new IllegalArgumentException("Duration must be greater than zero!");
        }

        long days = TimeUnit.MILLISECONDS.toDays(millis);
        millis -= TimeUnit.DAYS.toMillis(days);
        long hours = TimeUnit.MILLISECONDS.toHours(millis);
        millis -= TimeUnit.HOURS.toMillis(hours);
        long minutes = TimeUnit.MILLISECONDS.toMinutes(millis);
        millis -= TimeUnit.MINUTES.toMillis(minutes);
        long seconds = TimeUnit.MILLISECONDS.toSeconds(millis);

        StringBuilder sb = new StringBuilder(64);
        sb.append(days);
        sb.append(" Days ");
        sb.append(hours);
        sb.append(" Hours ");
        sb.append(minutes);
        sb.append(" Minutes ");
        sb.append(seconds);
        sb.append(" Seconds");

        return (sb.toString());
    }

    /**
     * method calculates number of days between two timestamps, presented by Epoch long numbers (number of millis)
     * @param timestampAlpha usually "smaller" number
     * @param timestampBeta usually "larger" number
     * @return integer presenting number of calendar days
     */
    public static int daysBetween(long timestampAlpha, long timestampBeta) {
        return Days.daysBetween(new DateTime(timestampAlpha), new DateTime(timestampBeta)).getDays();
    }

    /**
     * method calculates number of days in Epoch timestamp (number of millis)
     * @param timestamp number of milliseconds
     * @return integer presenting number of calendar days
     */
    public static int daysIn(long timestamp) {
        return daysBetween(0, timestamp);
    }
}
