package com.reinvent.synergy.data.primarykey;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Bohdan Mushkevych
 * date: 27 Sep 2011
 * Description: module contains common methods for primary key operations
 */
public class StringPrimaryKey extends AbstractPrimaryKey {
    private static final int KEY_LENGTH = 64;

    @Override
    protected int getPrimaryKeyLength() {
        return KEY_LENGTH;
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
