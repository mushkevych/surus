package com.reinvent.synergy.data.system;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Bohdan Mushkevych
 * Description: contains counters for the Synergy Map/Reduces
 */
public enum SynergyCounter {
    NUMBER_OF_INCONSISTENT_RECORDS,
    NUMBER_OF_OOM_ERRORS;

    public static void setCounter(Mapper.Context context, SynergyCounter identifier, long value) {
        Counter counter = context.getCounter(identifier);
        long currentValue = counter.getValue();
        if (currentValue < value) {
            currentValue = value - currentValue;
            counter.increment(currentValue);
        }
    }

}
