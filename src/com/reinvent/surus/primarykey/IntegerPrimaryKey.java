package com.reinvent.surus.primarykey;

import com.reinvent.surus.mapping.HFieldComponent;
import com.reinvent.surus.model.Constants;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Map;

/**
 * @author Bohdan Mushkevych
 * Description: module contains common methods for primary key operations
 */
public class IntegerPrimaryKey extends AbstractPrimaryKey {
    private static final HFieldComponent[] KEY_COMPONENTS = {new AnnotationRuntimeInstance() {
        @Override
        public String name() {
            return Constants.KEY;
        }

        @Override
        public int length() {
            return Bytes.SIZEOF_INT;
        }

        @Override
        public Class type() {
            return Integer.class;
        }
    }};

    @Override
    protected int getPrimaryKeyLength() {
        return KEY_COMPONENTS[0].length();
    }

    @Override
    public HFieldComponent[] getComponents() {
        return KEY_COMPONENTS;
    }

    @Override
    public ImmutableBytesWritable generateRowKey(Map<String, Object> components) {
        if (components.size() != KEY_COMPONENTS.length) {
            throw new IllegalArgumentException(String.format("Number of Key Components is incorrect %d vs %d", components.size(), KEY_COMPONENTS.length));
        }

        int key = (Integer) components.get(Constants.KEY);
        return generateKey(key);
    }

    /**
     * @param primaryKey the
     * @return String preseting IntegerPrimaryKey
     */
    public String toString(byte[] primaryKey) {
        return String.valueOf(Bytes.toInt(primaryKey));
    }

    /**
     * Parses integer value of the Primary Key
     * @param primaryKey the
     * @return value of the Key
     */
    public Integer getValue(byte[] primaryKey) {
        return Bytes.toInt(primaryKey);
    }

    /**
     * Method generated Primary Key base on Integer parameter
     * @param keyrow the
     * @return generated key
     */
    public ImmutableBytesWritable generateKey(Integer keyrow) {
        return new ImmutableBytesWritable(Bytes.toBytes(keyrow));
    }
}
