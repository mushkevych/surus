package com.reinvent.synergy.data.system;

import com.reinvent.synergy.data.model.Constants;
import org.apache.hadoop.hbase.client.Get;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Bohdan Mushkevych
 *         date: 30/08/11
 *         Description: Data Subset Context holds definition of column subsets
 */

public class SubsetContext {
    private final static HashMap<String, ColumnSubset> CONTEXT = new HashMap<String, ColumnSubset>();
    private static Logger log = Logger.getLogger(SubsetContext.class);

    public final static String ESSENTIAL_EXAMPLE = "essential_example";

    static {
        ColumnSubset subsetStatFamily = new ColumnSubset();
        subsetStatFamily.addFamily(Constants.FAMILY_STAT);
        CONTEXT.put(ESSENTIAL_EXAMPLE, subsetStatFamily);
    }

    public static ColumnSubset get(String subsetName) {
        if (!SubsetContext.CONTEXT.containsKey(subsetName)) {
            String msg = String.format("Subset %s is unknown to synergy-hadoop", subsetName);
            log.error(msg);
            throw new IllegalArgumentException(msg);
        }
        return SubsetContext.CONTEXT.get(subsetName);
    }
}
