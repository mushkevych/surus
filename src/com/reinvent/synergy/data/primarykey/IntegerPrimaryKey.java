package com.reinvent.synergy.data.primarykey;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Bohdan Mushkevych
 * date: 15 Dec 2011
 * Description: module contains common methods for primary key operations
 */
public class IntegerPrimaryKey extends AbstractPrimaryKey {
    private static final int KEY_LENGTH = Bytes.SIZEOF_INT;

    @Override
    protected int getPrimaryKeyLength() {
        return KEY_LENGTH;
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
