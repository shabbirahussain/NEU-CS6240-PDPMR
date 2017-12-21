package org.neu.pdpmr.tasks;

import org.apache.commons.cli.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.neu.pdpmr.tasks.subtasks.KScorer;

import java.io.*;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;
import org.neu.pdpmr.tasks.subtasks.LetterScorer;

/**
 * @author shabbir.ahussain
 */
public class Main extends Configured implements Tool {
    private static int k;
    private static String datDir;
    private static String outDir;
    private static String validCharRegex = "[^a-zA-Z]";

    private static Options options;

    public static void main(String[] args) throws Exception {
        // create Options object
        options = new Options();
        options.addOption("k", true, "K Neighborhood size.");
        options.addOption("datDir", true, "Input data directory.");
        options.addOption("outDir", true, "Path of output directory.");

        System.exit(ToolRunner.run(new Configuration(), new Main(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        System.out.println(Arrays.asList(args));

        Configuration conf = this.getConf();
        CommandLine cmd = new GnuParser().parse(options, args, true);
        try{
            k = Integer.parseInt(cmd.getOptionValue("k"));
            datDir = cmd.getOptionValue("datDir");
            outDir = cmd.getOptionValue("outDir");
        } catch (Exception e){
            e.printStackTrace();
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp( "ant", options );
            return -1;
        }

        taskA3();
        return 0;
    }

    /**
     * Generates and executes task 2.
     * @return time taken in milliseconds
     * @throws IOException when unable to read the input file.
     * @throws InterruptedException when thread is interrupted.
     */
    public void taskA3()
            throws IOException, InterruptedException, ClassNotFoundException {
        String letterOut = outDir + "/temp/letter-count";

        // Generate letter scores using map reduce.
        new LetterScorer(validCharRegex, letterOut).exec(datDir);

        letterOut += "/part-r-00000";
        // Generate word scores from map reduce
        new KScorer(validCharRegex, k, letterOut, outDir).exec(datDir);
    }

}
