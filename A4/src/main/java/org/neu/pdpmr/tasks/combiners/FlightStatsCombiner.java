package org.neu.pdpmr.tasks.combiners;

import org.apache.hadoop.mapreduce.Reducer;
import org.neu.pdpmr.tasks.types.FlightStatsKey;
import org.neu.pdpmr.tasks.types.FlightStatsValue;

import java.io.IOException;

/**
 * @author shabbir.ahussain
 */
public class FlightStatsCombiner
		extends Reducer<FlightStatsKey, FlightStatsValue, FlightStatsKey, FlightStatsValue> {
	/**
	 * Combines the values with sum.
	 *
	 * @param key is the part of composite key "key1Type", "key1".
	 * @param values are the values to be aggregated.
	 * @param context writes aggregated records for the keys to the context.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void reduce(FlightStatsKey key, Iterable<FlightStatsValue> values, Context context)
			throws IOException, InterruptedException {
		FlightStatsValue res = new FlightStatsValue();
		for (FlightStatsValue v : values) {
			res.delayInPer 	+= v.delayInPer;
			res.delayFreq 	+= v.delayFreq;
			res.flightCnt 	+= v.flightCnt;
		}
		context.write(key, res);
	}
}
