package org.neu.pdpmr.tasks;

import org.apache.commons.cli.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.*;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.neu.pdpmr.tasks.drivers.FilterTopNStats;
import org.neu.pdpmr.tasks.drivers.GatherFlightStats;

/**
 * @author shabbir.ahussain
 */
public class Main extends Configured implements Tool {
    private int n;
    private String datDir;
    private String outDir;
    private int startYear;

    private static Options options;

    public static void main(String[] args) throws Exception {
        // create Options object
        options = new Options();
        options.addOption("n", true, "Top N Airlines/Destinations.");
        options.addOption("datDir", true, "Input data directory.");
        options.addOption("outDir", true, "Path of output directory.");
        options.addOption("yearsSince", true, "Start year for data to filter.");

        System.exit(ToolRunner.run(new Configuration(), new Main(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.asList(args));

        Configuration conf = this.getConf();
        CommandLine cmd = new GnuParser().parse(options, args, true);
        try{
            this.n = Integer.parseInt(cmd.getOptionValue("n"));
            this.datDir = cmd.getOptionValue("datDir");
            this.outDir = cmd.getOptionValue("outDir");
            this.startYear  = Integer.parseInt(cmd.getOptionValue("yearsSince"));
        } catch (Exception e){
            e.printStackTrace();
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "ant", options );
            return -1;
        }

        taskA4();
        return 0;
    }

    /**
     * Generates and executes task 4.
     * @return time taken in milliseconds
     * @throws IOException when unable to read the input file.
     * @throws InterruptedException when thread is interrupted.
     */
    public void taskA4()
            throws IOException, InterruptedException, ClassNotFoundException {
        String tempDir = "/temp/flight-stats";

        // Generate gather flight stats
        new GatherFlightStats(tempDir, n, startYear).exec(datDir);

        // Filter top N stats (Airlines/Destinations)
        new FilterTopNStats(outDir).exec(tempDir);
    }

}
