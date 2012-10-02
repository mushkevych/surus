package com.reinvent.surus.system;

import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.*;

/**
 * @author Bohdan Mushkevych
 * Description: this module holds collection of column families and/or identifiers
 * that limit scope of update of retrieval from HBase, and thus - limitting IO/Network load
 */
public class ColumnSubset {
    final Set<String> families = new HashSet<String>();
    final Map<String, Set<String>> columns = new HashMap<String, Set<String>>();

    public ColumnSubset() {
    }

    public void addColumn(String family, String identifier) {
        if (!columns.containsKey(family)) {
            columns.put(family, new HashSet<String>());
        }
        Set<String> setOfColumns = columns.get(family);
        setOfColumns.add(identifier);
    }

    public void addFamily(String family) {
        families.add(family);
    }

    public Set<String> getFamilies() {
        return families;
    }

    public Map<String, Set<String>> getColumns() {
        return columns;
    }

    public boolean containsFamily(String family) {
        return families.contains(family);
    }

    public boolean containsColumn(String family, String identifier) {
        if (families.contains(family)) {
            return true;
        }

        if (columns.containsKey(family)) {
            Set<String> setOfColumns = columns.get(family);
            return setOfColumns.contains(identifier);
        } else {
            return false;
        }
    }

    public Get updateHBaseGet(Get get) {
        for (String family : getFamilies()) {
            get.addFamily(family.getBytes());
        }

        for (Map.Entry<String, Set<String>> entry : getColumns().entrySet()) {
            byte[] family = entry.getKey().getBytes();
            for (String identifier : entry.getValue()) {
                get.addColumn(family, identifier.getBytes());
            }
        }

        return get;
    }

    public Scan updateHBaseScan(Scan scan) {
        for (String family : getFamilies()) {
            scan.addFamily(family.getBytes());
        }

        for (Map.Entry<String, Set<String>> entry : getColumns().entrySet()) {
            byte[] family = entry.getKey().getBytes();
            for (String identifier : entry.getValue()) {
                scan.addColumn(family, identifier.getBytes());
            }
        }

        return scan;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("Families:");
        for (String family : families) {
            builder.append(family).append(",");
        }
        builder.append(System.getProperty("line.separator"));
        for (Map.Entry<String, Set<String>> entry : getColumns().entrySet()) {
            byte[] family = entry.getKey().getBytes();
            for (String identifier : entry.getValue()) {
                builder.append(Bytes.toString(family)).append(":").append(identifier);
            }
        }
        return builder.toString();
    }
}
