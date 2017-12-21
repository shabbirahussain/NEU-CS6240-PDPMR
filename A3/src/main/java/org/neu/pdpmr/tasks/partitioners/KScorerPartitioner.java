package org.neu.pdpmr.tasks.partitioners;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;
import org.neu.pdpmr.tasks.types.ScoreFrequencyPair;
import org.neu.pdpmr.tasks.types.WordKScoreTuple;

/**
 * @author shabbir.ahussain
 */
public class KScorerPartitioner
    extends Partitioner<WordKScoreTuple, ScoreFrequencyPair> {

    @Override
    public int getPartition(WordKScoreTuple key, ScoreFrequencyPair value, int numPartitions) {
        if (key.word.getLength() == 0) return 0;

        int lettersPerPart = (int) Math.floor(26 / numPartitions);
        return Math.min((key.word.charAt(0) - 'a')/ lettersPerPart, numPartitions - 1);
    }
}
