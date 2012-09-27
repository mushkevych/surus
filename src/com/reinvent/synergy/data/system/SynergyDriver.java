package com.reinvent.synergy.data.system;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author Bohdan Mushkevych
 * Description:
 */
public class SynergyDriver extends Configured implements Tool {
    private static final String PROPERTY_PROCESS_NAME = "process.name";
    private static final String PROPERTY_TIMEPERIOD_WORKING = "timeperiod.working";

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
//        conf.set("mapred.job.tracker", "local");

        String processName = conf.get(PROPERTY_PROCESS_NAME);
        int timePeriod = Integer.valueOf(conf.get(PROPERTY_TIMEPERIOD_WORKING));
        SynergyProcess process = ProcessContext.get(processName);
        JobRunner jobRunner = new JobRunner(process, timePeriod, conf);
        return jobRunner.executeHadoopJob();
    }

    public static void main(String[] args) {
        try {
            int exitCode = ToolRunner.run(new SynergyDriver(), args);
            System.exit(exitCode);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
