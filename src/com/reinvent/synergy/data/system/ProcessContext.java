package com.reinvent.synergy.data.system;

import com.reinvent.synergy.data.model.Constants;
import com.reinvent.synergy.data.primarykey.IntegerPrimaryKey;
import org.apache.hadoop.hbase.mapreduce.IdentityTableMapper;
import org.apache.hadoop.hbase.mapreduce.IdentityTableReducer;
import org.apache.log4j.Logger;

import java.util.HashMap;

/**
 * @author Bohdan Mushkevych
 *         Description: Process Context holds definition of all Synergy Processes
 */
public class ProcessContext {
    private final static HashMap<String, SynergyProcess> CONTEXT = new HashMap<String, SynergyProcess>();
    private static Logger log = Logger.getLogger(ProcessContext.class);

    public static final String PROCESS_EXAMPLE = "ExampleWorker";

    static {
        CONTEXT.put(PROCESS_EXAMPLE, new SynergyProcess(PROCESS_EXAMPLE,
                Constants.TABLE_EXAMPLE,
                Constants.TABLE_EXAMPLE,
                null,
                TimeQualifier.YEARLY,
                new IntegerPrimaryKey(),
                IdentityTableMapper.class,
                null,
                IdentityTableReducer.class));
    }

    public static SynergyProcess get(String processName) {
        if (!ProcessContext.CONTEXT.containsKey(processName)) {
            String msg = String.format("Process %s is unknown to synergy-hadoop", processName);
            log.error(msg);
            throw new IllegalArgumentException(msg);
        }
        return ProcessContext.CONTEXT.get(processName);
    }
}
