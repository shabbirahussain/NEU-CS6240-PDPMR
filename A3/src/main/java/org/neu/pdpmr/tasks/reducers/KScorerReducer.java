package org.neu.pdpmr.tasks.reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.neu.pdpmr.tasks.types.ScoreFrequencyPair;
import org.neu.pdpmr.tasks.types.WordKScoreTuple;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author shabbir.ahussain
 */
public class KScorerReducer
        extends Reducer<WordKScoreTuple, ScoreFrequencyPair, WordKScoreTuple, ScoreFrequencyPair> {
    @Override
    public void reduce(WordKScoreTuple key, Iterable<ScoreFrequencyPair> values,
                       Context context) throws IOException, InterruptedException {
        long frequency = 0;
        Iterator<ScoreFrequencyPair> itr = values.iterator();
        while(itr.hasNext()) {
            frequency += itr.next().frequency;
        }

        context.write(key, new ScoreFrequencyPair(key.score, frequency));
    }
}
