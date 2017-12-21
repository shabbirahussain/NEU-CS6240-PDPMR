package org.neu.pdpmr.tasks.combiners;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.neu.pdpmr.tasks.types.WordStatsWritable;

import java.io.IOException;

/**
 * @author shabbir.ahussain
 */
public class KScorerCombiner extends Reducer<Text, WordStatsWritable, Text, WordStatsWritable> {
    /**
     * Combines the given values into a single writable stats object.
     * @param values is the iterable of inputs from mappers.
     * @return Combined WordStatsWritable object from mappers.
     */
    public static WordStatsWritable getCombineWordStats(Iterable<WordStatsWritable> values){
        WordStatsWritable acc = new WordStatsWritable();
        values.forEach(v -> {
            acc.add(v.wordScore, v.wordCount);
        });
        return acc;
    }

    @Override
    public void reduce(Text key, Iterable<WordStatsWritable> values,
                       Context context) throws IOException, InterruptedException {
        context.write(key, KScorerCombiner.getCombineWordStats(values));
    }
}
