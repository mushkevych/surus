package com.reinvent.surus.primarykey;

import com.reinvent.surus.mapping.HFieldComponent;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import javax.enterprise.util.AnnotationLiteral;
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
     * @return list of HFieldComponents, defining the HRowKey
     */
    public abstract HFieldComponent[] getComponents();

    /**
     * Generate key based on transferred components
     * @param components in format Map<String, Object>, where:
     *                   - String presents human readable name of the component, same as from #getComponents
     *                   - Object presents initialized object of the
     * @return Key in format ImmutableBytesWritable
     */
    public abstract ImmutableBytesWritable generateRowKey(Map<String, Object> components);

    /**
     * Static class to handle runtime derivatives of the HFieldComponent annotations
     */
    static abstract class AnnotationRuntimeInstance extends AnnotationLiteral<HFieldComponent> implements HFieldComponent {}
}
