package org.neu.pdpmr.tasks.reducers;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.neu.pdpmr.tasks.types.ScoreFrequencyPair;
import org.neu.pdpmr.tasks.types.WordKScoreTuple;

import javax.security.auth.login.Configuration;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * @author shabbir.ahussain
 */
public class KScorerMedianReducer
        extends Reducer<WordKScoreTuple, ScoreFrequencyPair, Text, IntWritable> {
    @Override
    public void reduce(WordKScoreTuple key, Iterable<ScoreFrequencyPair> values,
                       Context context) throws IOException, InterruptedException {
        Iterator<ScoreFrequencyPair> itr = values.iterator();
        long length = itr.next().frequency;

        boolean isOdd = (length % 2) == 1;
        length /= 2;
        if (!isOdd) length--;   // For even use (a+b)/2.

        int sm = 0, sm1 = 0;
        while (length > 0) {
            final ScoreFrequencyPair next = itr.next();
            length -= next.frequency;
            sm = next.score;
        }

        if (length < -1) {           // Both values are prefetched in previous iteration.
            sm1 = sm;
        } else if (length == -1) {  // Only one value is prefetched.
            sm1 = (itr.hasNext())? itr.next().score: 0;
        } else if (length == 0 ){   // None of the values are prefetched.
            final ScoreFrequencyPair next = itr.next();
            sm = next.score;
            sm1 = (next.frequency > 1 || !itr.hasNext())? sm: itr.next().score;
        }

        if (isOdd) {
            context.write(key.word, new IntWritable(sm));
        } else {
            context.write(key.word,
                    new IntWritable((sm + sm1) / 2));
        }
    }
}
