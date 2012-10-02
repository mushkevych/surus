package com.reinvent.surus.system;

import com.reinvent.surus.model.Constants;
import org.apache.log4j.Logger;

import java.util.HashMap;

/**
 * @author Bohdan Mushkevych
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

    public static void register(String subsetName, ColumnSubset subset) {
        SubsetContext.CONTEXT.put(subsetName, subset);
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
