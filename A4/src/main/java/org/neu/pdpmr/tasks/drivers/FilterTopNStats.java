package org.neu.pdpmr.tasks.drivers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.neu.pdpmr.tasks.comparators.TopNGroupComparator;
import org.neu.pdpmr.tasks.reducers.FilterTopNReducer;
import org.neu.pdpmr.tasks.types.FlightStatsKey;
import org.neu.pdpmr.tasks.types.FlightStatsValue;

import java.io.IOException;

/**
 * @author shabbir.ahussain
 */
public class FilterTopNStats {
    private Configuration conf;
    private Path outPath;
    private FileSystem hdfs;

    /**
     * Default constructor.
     * @param outFile is the output file to store results to.
     * @throws IOException when unable to get hdfs instance.
     */
    public FilterTopNStats(final String outFile)
            throws IOException {
        this.outPath = new Path(outFile);

        conf = new Configuration();
        hdfs = FileSystem.newInstance(conf);
    }


    /**
     * Gets the statistics out of the corpus.
     * @param directory is the in cluster directory to read inputs from.
     * @throws IOException when unable to read the input file.
     * @throws InterruptedException when thread is interrupted.
     * @throws ClassNotFoundException when unable to read the serialized file.
     */
    public void exec(final String directory)
            throws IOException, InterruptedException, ClassNotFoundException {
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "Filter Top Airlines/Destinations");

        job.setJarByClass(FilterTopNStats.class);
        // Default mappers
        job.setReducerClass(FilterTopNReducer.class);

        job.setOutputKeyClass(FlightStatsKey.class);
        job.setOutputValueClass(FlightStatsValue.class);

        job.setGroupingComparatorClass(TopNGroupComparator.class);

        // delete existing files
        if (hdfs.exists(outPath)) {
            hdfs.delete(outPath, true);
        }

        FileInputFormat.addInputPath(job, new Path(directory));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.waitForCompletion(true);
    }
}
