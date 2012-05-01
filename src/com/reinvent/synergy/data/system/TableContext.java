package com.reinvent.synergy.data.system;

import com.reinvent.synergy.data.model.Constants;
import com.reinvent.synergy.data.model.Example;
import com.reinvent.synergy.data.primarykey.IntegerPrimaryKey;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;

/**
 * @author Bohdan Mushkevych
 * date 24/10/11
 * Description: holds relationship between table name, primary key and the pool manager
 */

class ContextMapping {
    TimeQualifier timeQualifier;
    PoolManager poolManager;

    ContextMapping(TimeQualifier timeQualifier, PoolManager poolManager) {
        this.timeQualifier = timeQualifier;
        this.poolManager = poolManager;
    }
}

public class TableContext {
    private final static Map<String, ContextMapping> CONTEXT = new HashMap<String, ContextMapping>();
    private static Logger log = Logger.getLogger(TableContext.class);

    static {
        CONTEXT.put(Constants.TABLE_EXAMPLE, new ContextMapping(
                TimeQualifier.HOURLY, new PoolManager<Example>(Constants.TABLE_EXAMPLE, Example.class, new IntegerPrimaryKey())));
    }

    private static ContextMapping getContextMapping(String tableName) {
        if (!TableContext.CONTEXT.containsKey(tableName)) {
            String msg = String.format("Table %s is unknown to synergy-hadoop", tableName);
            log.error(msg);
            throw new IllegalArgumentException(msg);
        }
        return TableContext.CONTEXT.get(tableName);
    }

    public static PoolManager getPoolManager(String tableName) {
        ContextMapping mapping = getContextMapping(tableName);
        return mapping.poolManager;
    }

    public static TimeQualifier getTimeQualifier(String tableName) {
        ContextMapping mapping = getContextMapping(tableName);
        return mapping.timeQualifier;
    }
}
