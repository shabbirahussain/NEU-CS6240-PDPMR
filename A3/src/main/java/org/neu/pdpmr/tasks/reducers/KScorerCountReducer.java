package org.neu.pdpmr.tasks.reducers;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.neu.pdpmr.tasks.types.ScoreFrequencyPair;
import org.neu.pdpmr.tasks.types.WordKScoreTuple;

import java.io.IOException;
import java.util.Iterator;

/**
 * @author shabbir.ahussain
 */
public class KScorerCountReducer
        extends Reducer<WordKScoreTuple, IntWritable, WordKScoreTuple, ScoreFrequencyPair> {
    @Override
    public void reduce(WordKScoreTuple key, Iterable<IntWritable> values,
                       Context context) throws IOException, InterruptedException {

        int length = 0;
        for(Iterator<IntWritable> itr = values.iterator();itr.hasNext();) {
            itr.next();
            length++;
        }
        key.score = -1; // To keep length at top while retrieving.
        context.write(key, new ScoreFrequencyPair(0, length));
    }
}
