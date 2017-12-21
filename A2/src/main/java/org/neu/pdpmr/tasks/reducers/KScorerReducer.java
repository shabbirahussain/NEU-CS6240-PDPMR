package org.neu.pdpmr.tasks.reducers;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.neu.pdpmr.tasks.combiners.KScorerCombiner;
import org.neu.pdpmr.tasks.types.WordStatsWritable;

import java.io.IOException;

/**
 * @author shabbir.ahussain
 */
public class KScorerReducer extends Reducer<Text, WordStatsWritable, Text, LongWritable> {
    @Override
    public void reduce(Text key, Iterable<WordStatsWritable> values,
                       Context context) throws IOException, InterruptedException {
        context.write(key, new LongWritable(KScorerCombiner
                .getCombineWordStats(values)
                .getMean()));
    }
}
