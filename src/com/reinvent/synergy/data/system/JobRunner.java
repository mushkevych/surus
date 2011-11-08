package com.reinvent.synergy.data.system;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @author Bohdan Mushkevych
 * date: 30/08/11
 * Description: module created and executes Hadoop Map/Reduce job
 */
public class JobRunner {
    public static final String PROCESS_NAME = "synergy.process.name";
    public static final String TIME_PERIOD= "synergy.timeperiod";

    private SynergyProcess process;
    private int timePeriod;
    private Configuration jobConfiguration;
    private static Logger log = Logger.getLogger(JobRunner.class);

    public JobRunner(SynergyProcess process, int timePeriod, Configuration jobConfiguration) {
        this.process = process;
        this.timePeriod = timePeriod;
        this.jobConfiguration = new Configuration(jobConfiguration);
        this.jobConfiguration.set(PROCESS_NAME, this.process.getName());
        this.jobConfiguration.set(TIME_PERIOD, String.valueOf(this.timePeriod));
    }

    protected Job createHadoopJob() throws IOException {
        String jobName = process.getName() + "_" + timePeriod;
        Job job = new Job(jobConfiguration, jobName);
        //job.setJarByClass(AbstractMapper.class);    // specifies jar with Synergy files

        byte[] startRow = null;
        byte[] stopRow = null;
        ColumnSubset subset = process.getSubsetSource();

        Scan scan = new Scan(startRow, stopRow);
        scan.setCacheBlocks(false);
        if (subset != null) {
            scan = subset.updateHBaseScan(scan);
        }

        TableMapReduceUtil.initTableMapperJob(process.getTableSource(),
                scan,
                process.getMapperClass(),
                ImmutableBytesWritable.class,
                Result.class,
                job);

        TableMapReduceUtil.initTableReducerJob(process.getTableTarget(),
                process.getReducerClass(),
                job);

        log.info(String.format("Starting computation for %s in %s; source: %s target: %s",
                process.getName(), timePeriod, process.getTableSource(), process.getTableTarget()));
        return job;
    }

    public int executeHadoopJob() {
        int returnCode = -100;
        try {
            Job job = createHadoopJob();
            returnCode = job.waitForCompletion(true) ? 0 : 1;
        } catch (IOException e) {
            log.error("IO Exception at Synergy Job Runner level", e);
        } catch (InterruptedException e) {
            log.error("Interrupted Exception at Synergy Job Runner level", e);
        } catch (ClassNotFoundException e) {
            log.error("Misconfiguration at Synergy Job Runner level", e);
        }
        return returnCode;
    }

}
