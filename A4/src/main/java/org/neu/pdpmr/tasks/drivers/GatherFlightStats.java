package org.neu.pdpmr.tasks.drivers;

import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.neu.pdpmr.tasks.combiners.FlightStatsCombiner;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.neu.pdpmr.tasks.mappers.FlightStatsMapper;
import org.neu.pdpmr.tasks.partitioners.FlightRecordPartitioner;
import org.neu.pdpmr.tasks.reducers.FlightStatsReducer;
import org.neu.pdpmr.tasks.types.FlightStatsKey;
import org.neu.pdpmr.tasks.types.FlightStatsValue;

/**
 * @author shabbir.ahussain
 */
public class GatherFlightStats {
    private Configuration conf;
    private Path outPath;
    private FileSystem hdfs;

    /**
     * Default constructor.
     * @param outFile is the output file to store results to.
     * @param n is the top n records to filter.
     * @param startYear is the starting year for data to filter.
     * @throws IOException when unable to get hdfs instance.
     */
    public GatherFlightStats(final String outFile, final int n, final int startYear)
            throws IOException {
        this.outPath = new Path(outFile);

        conf = new Configuration();
        conf.setInt("N", n);
        conf.setInt("startYear", startYear);
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
        Job job = Job.getInstance(conf, "Flights Stats Main Job");

        job.setJarByClass(GatherFlightStats.class);
        job.setMapperClass(FlightStatsMapper.class);
        job.setCombinerClass(FlightStatsCombiner.class);
        job.setReducerClass(FlightStatsReducer.class);

        job.setOutputKeyClass(FlightStatsKey.class);
        job.setOutputValueClass(FlightStatsValue.class);
        job.setPartitionerClass(FlightRecordPartitioner.class);


        // delete existing files
        if (hdfs.exists(outPath)) {
            hdfs.delete(outPath, true);
        }

        FileInputFormat.addInputPath(job, new Path(directory));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, outPath);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);

        job.waitForCompletion(true);
    }
}
