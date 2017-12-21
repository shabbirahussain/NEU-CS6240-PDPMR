package org.neu.pdpmr.tasks.drivers;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.neu.pdpmr.tasks.mappers.TwoHopFlightsMapper;
import org.neu.pdpmr.tasks.reducers.TwoHopsFlightsReducer;
import org.neu.pdpmr.tasks.types.Flight;
import org.neu.pdpmr.tasks.types.FlightTuple;
import org.neu.pdpmr.tasks.types.Query;

import java.io.IOException;
import java.util.Collection;

/**
 * @author shabbir.ahussain
 */
public class Genrate2HopTuples {
    private Configuration conf;
    private Path outPath, inPath;
    private FileSystem hdfs;

    /**
     * Default constructor.
     * @param outFile is the output file to store results to.
     * @param queries is the list of queries to find.
     * @throws IOException when unable to get hdfs instance.
     */
    public Genrate2HopTuples(final String inFile,
                             final String outFile,
                             final Collection<Query> queries)
            throws IOException  {
        this.outPath = new Path(outFile);
        this.inPath  = new Path(inFile);

        conf = new Configuration();
        conf.set("queries", new Gson().toJson(queries));
        hdfs = FileSystem.newInstance(conf);
    }

    /**
     * Generates 2 hops flights.
     * @throws IOException when unable to read the input file.
     * @throws InterruptedException when thread is interrupted.
     * @throws ClassNotFoundException when unable to read the serialized file.
     */
    public void exec()
            throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, "2 Hop Flight Tuples Generator.");

        job.setJarByClass(Genrate2HopTuples.class);
        job.setMapperClass(TwoHopFlightsMapper.class);
        job.setReducerClass(TwoHopsFlightsReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Flight.class);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(FlightTuple.class);

        // delete existing files
        if (this.hdfs.exists(this.outPath)) {
            this.hdfs.delete(this.outPath, true);
        }

        FileInputFormat.addInputPath(job, this.inPath);
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, this.outPath);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setNumReduceTasks(1);

        job.waitForCompletion(true);
    }
}
