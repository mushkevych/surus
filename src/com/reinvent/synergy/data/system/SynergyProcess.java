package com.reinvent.synergy.data.system;

import com.reinvent.synergy.data.primarykey.AbstractPrimaryKey;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;

/**
 * @author Bohdan Mushkevych
 *         Description: defines common properties for SynergyProcess
 */
public class SynergyProcess {
    private String name;
    private String tableSource;
    private String tableTarget;
    private ColumnSubset subsetSource;
    private TimeQualifier qualifier;
    private AbstractPrimaryKey primaryKey;
    private Class<? extends TableMapper> mapper;
    private Class combiner;
    private Class<? extends TableReducer> reducer;

    public SynergyProcess(String name,
                          String tableSource,
                          String tableTarget,
                          ColumnSubset subsetSource,
                          TimeQualifier qualifier,
                          AbstractPrimaryKey primaryKey,
                          Class<? extends TableMapper> mapper,
                          Class combiner,
                          Class<? extends TableReducer> reducer) {
        this.name = name;
        this.tableSource = tableSource;
        this.tableTarget = tableTarget;
        this.subsetSource = subsetSource;
        this.qualifier = qualifier;
        this.primaryKey = primaryKey;
        this.mapper = mapper;
        this.combiner = combiner;
        this.reducer = reducer;
    }

    public String getName() {
        return name;
    }

    public String getTableSource() {
        return tableSource;
    }

    public String getTableTarget() {
        return tableTarget;
    }

    public ColumnSubset getSubsetSource() {
        return subsetSource;
    }

    public TimeQualifier getQualifier() {
        return qualifier;
    }

    public AbstractPrimaryKey getPrimaryKey() {
        return primaryKey;
    }

    public Class<? extends TableMapper> getMapperClass() {
        return mapper;
    }

    public Class getCombinerClass() {
        return combiner;
    }

    public Class<? extends TableReducer> getReducerClass() {
        return reducer;
    }
}

