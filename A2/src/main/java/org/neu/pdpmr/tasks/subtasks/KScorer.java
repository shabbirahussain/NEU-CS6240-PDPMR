package org.neu.pdpmr.tasks.subtasks;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.neu.pdpmr.tasks.combiners.KScorerCombiner;
import org.neu.pdpmr.tasks.mappers.KScorerMapper;
import org.neu.pdpmr.tasks.reducers.KScorerReducer;
import org.neu.pdpmr.tasks.types.WordStatsWritable;

import java.io.IOException;

/**
 * @author shabbir.ahussain
 */
public class KScorer {
    private Configuration conf;
    private Path outPath;
    private FileSystem hdfs;

    /**
     * Default constructor
     * @param validCharsRegex is the valid characters regex to screen bad characters in file.
     * @param kNeighborhood is the k neighborhood to generate score for.
     * @param letterScoresFile is the letters scores file to read from.
     * @param outFile is the output file to write to.
     */
    public KScorer(final String validCharsRegex,
                   final Integer kNeighborhood,
                   final String letterScoresFile,
                   final String outFile)
            throws IOException {
        conf = new Configuration();
        conf.set("validCharsRegex", validCharsRegex);
        conf.set("kNeighborhood", kNeighborhood.toString());
        conf.set("letterScoresFile", letterScoresFile);
        hdfs = FileSystem.newInstance(conf);

        outPath = new Path(outFile);
    }

    /**
     * Gets the word's k-score from the corpus.
     * @param directory is the in cluster directory to read inputs from.
     * @throws IOException when unable to read the input file.
     * @throws InterruptedException when thread is interrupted.
     */
    public void generateScoreFromCorpus(final String directory)
            throws IOException, InterruptedException, ClassNotFoundException {
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "K Scorer");

        job.setJarByClass(KScorer.class);
        job.setMapperClass(KScorerMapper.class);
        job.setMapOutputValueClass(WordStatsWritable.class);
        job.setCombinerClass(KScorerCombiner.class);
        job.setReducerClass(KScorerReducer.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        // delete existing files
        if (hdfs.exists(outPath)) {
            hdfs.delete(outPath, true);
        }

        FileInputFormat.addInputPath(job, new Path(directory));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, outPath);
        job.setOutputFormatClass(TextOutputFormat.class);

        System.out.println("starting job");
        job.waitForCompletion(true);
    }
}
