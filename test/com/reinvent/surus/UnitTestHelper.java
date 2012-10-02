package com.reinvent.surus;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author Bohdan Mushkevych
 * Description: helper providing methods to create and delete tables "on-fly"
 */
public class UnitTestHelper {
    /**
     * method creates table with given name and variable number of family-column structure
     * @param jobConfiguration Job configuration that used to initialize HBaseAdmin
     * @param tableName table to create
     * @param columnFamilies to create
     * @throws java.io.IOException the
     */
    public static void createTable(Configuration jobConfiguration, String tableName, String ... columnFamilies) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(jobConfiguration);
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        for (String family: columnFamilies) {
            HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
            tableDescriptor.addFamily(columnDescriptor);
        }
        admin.createTable(tableDescriptor);

        HTableDescriptor[] tableList = admin.listTables();
        if (tableList.length != 1 && Bytes.equals(tableDescriptor.getName(), tableList[0].getName())) {
            throw new IOException("Failed to create table: " + tableName);
        }
    }

    /**
     * method deletes table with given name
     * @param jobConfiguration Job configuration that used to initialize HBaseAdmin
     * @param tableName table to delete
     * @throws java.io.IOException the
     */
    public static void deleteTable(Configuration jobConfiguration, String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(jobConfiguration);
        HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);
        admin.disableTable(tableDescriptor.getName());
        admin.deleteTable(tableDescriptor.getName());
    }
}
