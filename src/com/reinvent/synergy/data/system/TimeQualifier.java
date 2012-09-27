package com.reinvent.synergy.data.system;

/**
 * @author Bohdan Mushkevych
 * Description: Module holds all supported TimeQualifiers by the System.
 */
public enum TimeQualifier {
    REAL_TIME(TimeQualifier.STR_REAL_TIME, null, TimeQualifier.NUMBER_OF_POSITIONS),
    BY_SCHEDULE(TimeQualifier.STR_BY_SCHEDULE, null, TimeQualifier.NUMBER_OF_POSITIONS),
    HOURLY(TimeQualifier.STR_HOURLY, TimeQualifier.PATTERN_HOURLY, TimeQualifier.NUMBER_OF_POSITIONS),
    DAILY(TimeQualifier.STR_DAILY, TimeQualifier.PATTERN_DAILY, TimeQualifier.NUMBER_OF_POSITIONS - 2),
    MONTHLY(TimeQualifier.STR_MONTHLY, TimeQualifier.PATTERN_MONTHLY, TimeQualifier.NUMBER_OF_POSITIONS - 4),
    YEARLY(TimeQualifier.STR_YEARLY, TimeQualifier.PATTERN_YEARLY, TimeQualifier.NUMBER_OF_POSITIONS - 6);

    public static final String PATTERN_PARSER =  "yyyyMMddHH";
    public static final String PATTERN_HOURLY =  "yyyyMMddHH";
    public static final String PATTERN_DAILY =   "yyyyMMdd00";
    public static final String PATTERN_MONTHLY = "yyyyMM0000";
    public static final String PATTERN_YEARLY =  "yyyy000000";

    public static final String STR_REAL_TIME = "real_time";
    public static final String STR_BY_SCHEDULE = "by_schedule";
    public static final String STR_HOURLY = "_hourly";
    public static final String STR_DAILY = "_daily";
    public static final String STR_MONTHLY = "_monthly";
    public static final String STR_YEARLY = "_yearly";
    public static final int NUMBER_OF_POSITIONS = 10;

    private final String representation;
    private final String pattern;
    private final int meaningfulPositions;

    TimeQualifier(String representation, String pattern, int meaningfulPositions) {
        this.representation = representation;
        this.pattern = pattern;
        this.meaningfulPositions = meaningfulPositions;
    }

    public int getMeaningfulPositions() {
        return meaningfulPositions;
    }

    public String getRepresentation() {
        return representation;
    }

    public String getPattern() {
        return pattern;
    }

    public static TimeQualifier parseString(String value) {
        if (REAL_TIME.representation.equals(value)) {
            return REAL_TIME;
        } else if (BY_SCHEDULE.representation.equals(value)) {
            return BY_SCHEDULE;
        } else if (HOURLY.representation.equals(value)) {
            return HOURLY;
        } else if (DAILY.representation.equals(value)) {
            return DAILY;
        } else if (MONTHLY.representation.equals(value)) {
            return MONTHLY;
        } else if (YEARLY.representation.equals(value)) {
            return YEARLY;
        } else {
            throw new IllegalArgumentException("Can not find match for: " + value);
        }
    }
}
