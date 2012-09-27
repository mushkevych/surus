package com.reinvent.synergy.data.primarykey;

import com.reinvent.synergy.data.model.Constants;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Bohdan Mushkevych
 * Description: module contains common methods for primary key operations
 */
public class StringPrimaryKey extends AbstractPrimaryKey {
    private static final int KEY_LENGTH = 64;
    private static final Map<String, Class> KEY_COMPONENTS = new HashMap<String, Class>();
    static {
        KEY_COMPONENTS.put(Constants.KEY, String.class);
    }

    @Override
    protected int getPrimaryKeyLength() {
        return KEY_LENGTH;
    }

    @Override
    public Map<String, Class> getComponents() {
        return KEY_COMPONENTS;
    }

    @Override
    public ImmutableBytesWritable generateRowKey(Map<String, Object> components) {
        if (components.size() != KEY_COMPONENTS.size()) {
            throw new IllegalArgumentException(String.format("Number of Key Components is incorrect %d vs %d", components.size(), KEY_COMPONENTS.size()));
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
