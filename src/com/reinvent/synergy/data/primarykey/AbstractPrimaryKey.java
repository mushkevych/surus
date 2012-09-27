package com.reinvent.synergy.data.primarykey;

import com.reinvent.synergy.data.system.TimeQualifier;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

/**
 * @author Bohdan Mushkevych
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

    /**
     * Return map, where:
     * - key is human readable name of the Key component (for example: "timeperiod", "domain_name", etc)
     * - value is the class of the component (for example: "timeperiod" : Integer.class)
     * @return Map in format <Human Readable Name of the Component: Component Class.
     * For example: <"timeperiod" : Integer.class> <"domain_name" : String.class>
     */
    public abstract Map<String, Class> getComponents();

    /**
     * Generate key based on transferred components
     * @param components in format Map<String, Object>, where:
     *                   - String presents human readable name of the component, same as from @getComponents
     *                   - Object presents initialized object of the
     * @return Key in format ImmutableBytesWritable
     */
    public abstract ImmutableBytesWritable generateRowKey(Map<String, Object> components);
}
