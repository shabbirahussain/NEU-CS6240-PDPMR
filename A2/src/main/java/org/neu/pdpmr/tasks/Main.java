package org.neu.pdpmr.tasks;

import org.apache.commons.cli.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.neu.pdpmr.tasks.subtasks.KScorer;
import org.neu.pdpmr.tasks.subtasks.LetterScorer;

import java.io.*;
import java.util.Arrays;

import org.apache.commons.cli.CommandLine;

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
        //String extraArgs[] = new GenericOptionsParser(conf, args).getRemainingArgs();
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

        taskA2();
        return 0;
    }

    /**
     * Generates and executes task 2.
     * @return time taken in milliseconds
     * @throws IOException when unable to read the input file.
     * @throws InterruptedException when thread is interrupted.
     */
    public void taskA2()
            throws IOException, InterruptedException, ClassNotFoundException {
        String tempOutFile = outDir + "/letter-count";

        // Generate letter scores using map reduce.
        new LetterScorer(validCharRegex, tempOutFile).generateScoreFromCorpus(datDir);

        // Generate word scores from map reduce
        new KScorer(validCharRegex, k, tempOutFile + "/part-r-00000", outDir + "/k-scores")
                .generateScoreFromCorpus(datDir);
    }

}
