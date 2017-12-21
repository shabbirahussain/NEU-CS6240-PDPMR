package org.neu.pdpmr.tasks.comparators;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.neu.pdpmr.tasks.types.FlightStatsKey;

/**
 * Clubs all records by key1Type and key1.
 * 
 * @author shabbir.ahussain
 */
public class TopNGroupComparator extends WritableComparator {
    protected TopNGroupComparator() {
        super(FlightStatsKey.class, true);
    }

    @Override
    public int compare(WritableComparable w1, WritableComparable w2) {
        FlightStatsKey k1 = (FlightStatsKey) w1;
        FlightStatsKey k2 = (FlightStatsKey) w2;
        int res = 0;
        if ((res = k1.key1Type.compareTo(k2.key1Type)) != 0) return res;
        return k1.key1.compareTo(k2.key1);
    }
}
