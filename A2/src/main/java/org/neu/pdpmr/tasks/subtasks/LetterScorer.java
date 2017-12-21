package org.neu.pdpmr.tasks.subtasks;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.LongSumReducer;
import org.neu.pdpmr.tasks.mappers.LetterCounterMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author shabbir.ahussain
 */
public class LetterScorer {
    private Configuration conf;
    private Path outPath;
    private FileSystem hdfs;

    /**
     * Default constructor.
     * @param validCharsRegex is the valid characters regex to screen bad characters in file.
     * @param outFile is the output file to store results to.
     * @throws IOException
     */
    public LetterScorer(final String validCharsRegex, final String outFile)
            throws IOException {
        this.outPath = new Path(outFile);

        conf = new Configuration();
        conf.set("validCharsRegex", validCharsRegex);
        hdfs = FileSystem.newInstance(conf);
    }


    /**
     * Gets the character score from the corpus.
     * @param directory is the in cluster directory to read inputs from.
     * @return Character map of characters and scores.
     * @throws IOException when unable to read the input file.
     * @throws InterruptedException when thread is interrupted.
     * @throws ClassNotFoundException
     */
    public void generateScoreFromCorpus(final String directory)
            throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, "Letter Counter");

        job.setJarByClass(LetterScorer.class);
        job.setMapperClass(LetterCounterMapper.class);
        job.setReducerClass(LongSumReducer.class);

        job.setNumReduceTasks(1);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);

        // delete existing files
        if (hdfs.exists(outPath)) {
            hdfs.delete(outPath, true);
        }

        FileInputFormat.addInputPath(job, new Path(directory));
        FileInputFormat.setInputDirRecursive(job, true);
        FileOutputFormat.setOutputPath(job, outPath);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        job.waitForCompletion(true);
    }

    /**
     * Reads pre-generated letter scores.
     * @return A map of character and their scores.
     * @throws IOException
     */
    public Map<Character, Integer> readPreGeneratedLetterScores()
            throws IOException {
        return getLetterScoreFromPercentage(readMROutput(outPath));
    }

    /**
     * Reads the output from letter-counter mr job into a map.
     * @param path is the path of sequencefile to read.
     * @return A map of character and their scores.
     * @throws IOException
     */
    private Map<Character, Long> readMROutput(final Path path) throws IOException {
        SequenceFile.Reader reader = new SequenceFile.Reader(conf, SequenceFile.Reader.file(path));
        IntWritable  key   = new IntWritable();
        LongWritable value = new LongWritable();
        Map<Character, Long> resultMap = new HashMap<>();
        while (reader.next(key, value)) {
            resultMap.put((char) key.get(), value.get());
        }
        reader.close();
        return resultMap;
    }

    /**
     * Generates a map of scores based on the rules of percentage occurrence specified in the assignement specs.
     *
     * @param cntMap is the input map of count of characters.
     * @return A map of characters and scores.
     */
    private Map<Character, Integer> getLetterScoreFromPercentage(final Map<Character, Long> cntMap) {
        double tot = (double) cntMap.values().stream().mapToLong(i -> i).sum();
        return cntMap.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> {
                            double score = (e.getValue() / tot);
                            if (score >= 0.10) return 0;
                            if (score >= 0.08) return 1;
                            if (score >= 0.06) return 2;
                            if (score >= 0.04) return 4;
                            if (score >= 0.02) return 8;
                            if (score >= 0.01) return 16;
                            return 32;
                        })
                );
    }
}
