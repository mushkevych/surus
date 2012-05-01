package com.reinvent.synergy.data.primarykey;

import com.reinvent.synergy.data.system.TimeQualifier;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Bohdan Mushkevych
 * date: 27 Sep 2011
 * Description: module contains common methods for primary key operations
 */
public abstract class AbstractPrimaryKey {
    /**
     * @return number of bytes in the key
     */
    protected abstract int getPrimaryKeyLength();

    /**
     * Parses PrimaryKey to human readable form
     * @param primaryKey byte[] of the HBase row
     * @return String in format "timePeriod SiteName"
     */
    public abstract String toString(byte[] primaryKey);
}
