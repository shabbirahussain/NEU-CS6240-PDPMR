package org.neu.pdpmr.tasks.partitioners;

import org.apache.hadoop.mapreduce.Partitioner;
import org.neu.pdpmr.tasks.types.FlightStatsKey;
import org.neu.pdpmr.tasks.types.FlightStatsValue;

/**
 * @author shabbir.ahussain
 */
public class FlightRecordPartitioner
    extends Partitioner<FlightStatsKey, FlightStatsValue> {

    @Override
    public int getPartition(FlightStatsKey key, FlightStatsValue value, int numPartitions) {
        return (key.key1Type.hashCode() * 127 + key.key1.hashCode()) % numPartitions;
    }
}
