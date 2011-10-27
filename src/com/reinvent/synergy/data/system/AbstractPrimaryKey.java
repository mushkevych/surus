package com.reinvent.synergy.data.system;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Bohdan Mushkevych
 * date: 27 Sep 2011
 * Description: module contains methods to generate Exemplary primary key
 */
public class AbstractPrimaryKey {
    public static final int DOMAIN_NAME_LENGTH = 253;
    private static final int VPK_LENGTH = Bytes.SIZEOF_INT + DOMAIN_NAME_LENGTH;

    public int getPrimaryKeyLength() {
        return VPK_LENGTH;
    }

    /**
     * Method generated Primary Key base on domain name and timeperiod
     * @param timePeriod
     * @param domainName
     * @param qualifier
     * @return generated key
     */
    public ImmutableBytesWritable generateKey(int timePeriod, String domainName, TimeQualifier qualifier) {
        byte[] primaryKey = new byte[getPrimaryKeyLength()];
        Bytes.putBytes(primaryKey, 0, Bytes.toBytes(timePeriod), 0, Bytes.SIZEOF_INT);
        Bytes.putBytes(primaryKey, Bytes.SIZEOF_INT, Bytes.toBytes(domainName), 0, domainName.length());
        return new ImmutableBytesWritable(primaryKey);
    }

    /**
     * Parses PrimaryKey and extracts Domain Name from it
     * @param primaryKey
     * @return String preseting domain name
     */
    public String getDomainName(byte[] primaryKey) {
        String domainName = Bytes.toString(primaryKey, Bytes.SIZEOF_INT, DOMAIN_NAME_LENGTH);
        return domainName.trim();
    }

    /**
     * Parses PrimaryKey to human readable form
     * @param primaryKey
     * @return String in format "timePeriod SiteName"
     */
    public String toString(byte[] primaryKey) {
        int timePeriod = Bytes.toInt(primaryKey, 0, Bytes.SIZEOF_INT);
        String domainName = Bytes.toString(primaryKey, Bytes.SIZEOF_INT, DOMAIN_NAME_LENGTH);
        return String.format("%d %s", timePeriod, domainName.trim());
    }
}
