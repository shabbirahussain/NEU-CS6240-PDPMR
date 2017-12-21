package org.neu.pdpmr.tasks.subtasks;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.neu.pdpmr.tasks.comparators.KScoreGroupComparator;
import org.neu.pdpmr.tasks.inputformats.NonSplittableTextInputFormat;
import org.neu.pdpmr.tasks.mappers.KScorerMapper;
import org.neu.pdpmr.tasks.partitioners.KScorerPartitioner;
import org.neu.pdpmr.tasks.reducers.KScorerCountReducer;
import org.neu.pdpmr.tasks.reducers.KScorerMedianReducer;
import org.neu.pdpmr.tasks.reducers.KScorerReducer;
import org.neu.pdpmr.tasks.types.ScoreFrequencyPair;
import org.neu.pdpmr.tasks.types.WordKScoreTuple;

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
     * @param validCharsRegex is the valid characters to pick.
     * @param k is the k neighborhood to generate score for.
     * @param letterScoresFile is the letters scores file to read from.
     * @param outFile is the output file to write to.
     */
    public KScorer(final String validCharsRegex,
                   final Integer k,
                   final String letterScoresFile,
                   final String outFile)
            throws IOException {
        conf = new Configuration();
        conf.setInt("kNeighborhood", k);
        conf.set("letterScoresFile", letterScoresFile);
        conf.set("validCharsRegex", validCharsRegex);
        hdfs = FileSystem.newInstance(conf);

        outPath = new Path(outFile);
    }

    /**
     * Gets the word's k-score from the corpus.
     * @param directory is the in cluster directory to read inputs from.
     * @throws IOException when unable to read the input file.
     * @throws InterruptedException when thread is interrupted.
     */
    public void exec(final String directory)
            throws IOException, InterruptedException, ClassNotFoundException {
        Path rawK = new Path(outPath.toString() + "/temp/raw-k");
        Path kCnt = new Path(outPath.toString() + "/temp/k-cnt");
        Path kFin = new Path(outPath.toString() + "/k-final");

        this.execKScoring(directory, rawK);
        this.execCountReducing(rawK.toString(), kCnt);
        this.execKReducing(rawK.toString() + "," + kCnt.toString(), kFin);
    }

    /**
     * Generates the inital neighborhood k scores.
     * @param directory is the input directory.
     * @param outPath is the output directory path.
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private void execKScoring(final String directory, final Path outPath)
            throws IOException, InterruptedException, ClassNotFoundException{
        Job job = Job.getInstance(conf, "K Scorer: Generating K Scores.");

        job.setJarByClass(KScorer.class);

        job.setInputFormatClass(NonSplittableTextInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.setMapperClass(KScorerMapper.class);
        job.setCombinerClass(KScorerReducer.class);
        job.setReducerClass(KScorerReducer.class);

        job.setMapOutputKeyClass(WordKScoreTuple.class);
        job.setMapOutputValueClass(ScoreFrequencyPair.class);

        job.setOutputKeyClass(WordKScoreTuple.class);
        job.setOutputValueClass(ScoreFrequencyPair.class);

        job.setPartitionerClass(KScorerPartitioner.class);

        // delete existing files
        if (hdfs.exists(outPath)) {
            hdfs.delete(outPath, true);
        }

        FileInputFormat.addInputPath(job, new Path(directory));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, outPath);

        job.waitForCompletion(true);
    }

    /**
     * Generates count of records emitted for every word.
     * @param directory is the input directory.
     * @param outPath is the output directory path.
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private void execCountReducing(final String directory, final Path outPath)
            throws IOException, InterruptedException, ClassNotFoundException{
        Job job = Job.getInstance(conf, "K Scorer: Generating K counts.");

        job.setJarByClass(KScorer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Default mapper
        job.setReducerClass(KScorerCountReducer.class);

        job.setMapOutputKeyClass(WordKScoreTuple.class);
        job.setMapOutputValueClass(ScoreFrequencyPair.class);

        job.setOutputKeyClass(WordKScoreTuple.class);
        job.setOutputValueClass(ScoreFrequencyPair.class);

        job.setPartitionerClass(KScorerPartitioner.class);
        job.setGroupingComparatorClass(KScoreGroupComparator.class);

        // delete existing files
        if (hdfs.exists(outPath)) {
            hdfs.delete(outPath, true);
        }

        FileInputFormat.addInputPath(job, new Path(directory));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, outPath);

        job.waitForCompletion(true);
    }

    /**
     * Performs a reduce side join to find median.
     * @param directories is comma seprated directories
     * @param outPath is the output directory path.
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private void execKReducing(final String directories, final Path outPath)
            throws IOException, InterruptedException, ClassNotFoundException{
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "K Scorer: Finding K scores median.");

        job.setJarByClass(KScorer.class);

        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        // Default mapper
        job.setReducerClass(KScorerMedianReducer.class);

        job.setMapOutputKeyClass(WordKScoreTuple.class);
        job.setMapOutputValueClass(ScoreFrequencyPair.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setPartitionerClass(KScorerPartitioner.class);
        job.setGroupingComparatorClass(KScoreGroupComparator.class);

        // delete existing files
        if (hdfs.exists(outPath)) {
            hdfs.delete(outPath, true);
        }

        FileInputFormat.addInputPaths(job, directories);
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, outPath);

        job.waitForCompletion(true);
    }
}
