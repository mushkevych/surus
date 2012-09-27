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
public class IntegerPrimaryKey extends AbstractPrimaryKey {
    private static final int KEY_LENGTH = Bytes.SIZEOF_INT;
    private static final Map<String, Class> KEY_COMPONENTS = new HashMap<String, Class>();
    static {
        KEY_COMPONENTS.put(Constants.KEY, Integer.class);
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
