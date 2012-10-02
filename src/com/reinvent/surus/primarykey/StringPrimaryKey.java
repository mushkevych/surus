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
public class StringPrimaryKey extends AbstractPrimaryKey {
    private static final HFieldComponent[] KEY_COMPONENTS = {new AnnotationRuntimeInstance() {
        private static final int KEY_LENGTH = 64;

        @Override
        public String name() {
            return Constants.KEY;
        }

        @Override
        public int length() {
            return KEY_LENGTH;
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

        String key = (String) components.get(Constants.KEY);
        return generateKey(key);
    }

    /**
     * Parses PrimaryKey and extracts Domain Name from it
     * @param primaryKey the
     * @return String preseting domain name
     */
    public String toString(byte[] primaryKey) {
        return Bytes.toString(primaryKey).trim();
    }

    /**
     * Method generated Primary Key base on String parameter
     * @param keyrow the
     * @return generated key
     */
    public ImmutableBytesWritable generateKey(String keyrow) {
        if (keyrow.length() > getPrimaryKeyLength()) {
            keyrow = keyrow.substring(0, getPrimaryKeyLength());
        }

        byte[] primaryKey = new byte[getPrimaryKeyLength()];
        Bytes.putBytes(primaryKey, 0, Bytes.toBytes(keyrow), 0, keyrow.length());
        return new ImmutableBytesWritable(primaryKey);
    }
}
