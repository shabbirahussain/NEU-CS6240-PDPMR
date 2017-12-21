package org.neu.pdpmr.tasks.reducers;

import org.apache.hadoop.mapreduce.Reducer;
import org.neu.pdpmr.tasks.types.FlightStatsKey;
import org.neu.pdpmr.tasks.types.FlightStatsValue;

import java.io.IOException;
import java.util.*;

/**
 * Filters top N airlines/destinations from the input stream.
 * These are identified by a reduce side join where first will have -1 in the frequency,
 * if it is part of top N collection.
 *
 * @author shabbir.ahussain
 */
public class FilterTopNReducer
		extends Reducer<FlightStatsKey, FlightStatsValue, FlightStatsKey, FlightStatsValue> {
	/**
	 * Performs reduce side join expects top record in the values to signify if that "key1Type",
	 * and "key1" combination is to be piped further in pipeline. It checks the "delayFreq" of the
	 * first value to check if it is -1. Here presence of -1 signifies that key1 is selected in top
	 * N by ranking. Otherwise ignores all the values for that key.
	 *
	 * @param k is a composite key aggregated at "key1Type" and "key1" using Group Comparator.
	 * @param values Flight delay stats we wanna extract, plus the remaining keys unused in group
	 *               comparator. "key2", "yearMonth".
	 * @param context Passes through FlightStatsValue back for successfully joined records.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void reduce(FlightStatsKey k, Iterable<FlightStatsValue> values, Context context)
			throws IOException, InterruptedException {

		final Iterator<FlightStatsValue> itr = values.iterator();
		FlightStatsValue v = itr.next();
		if (v.delayFreq != -1) return; // These records don't have a top pick indicator at the top

		while(itr.hasNext()){
			v = itr.next();
			context.write(new FlightStatsKey(k.key1Type, k.key1, v.key2, v.yearMonth),
					new FlightStatsValue(v.delayInPer, v.delayFreq, v.flightCnt));
		}
	}
}
