package org.neu.pdpmr.tasks.comparators;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.neu.pdpmr.tasks.types.WordKScoreTuple;

/**
 * @author shabbir.ahussain
 */
public class KScoreGroupComparator extends WritableComparator {
    protected KScoreGroupComparator() {
        super(WordKScoreTuple.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        return ((WordKScoreTuple) w1)
                .word
                .compareTo(((WordKScoreTuple) w2).word);
    }
}
