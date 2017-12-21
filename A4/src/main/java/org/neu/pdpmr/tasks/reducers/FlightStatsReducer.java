package org.neu.pdpmr.tasks.reducers;

import java.io.IOException;

import java.util.*;

import com.google.common.util.concurrent.AtomicLongMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.neu.pdpmr.tasks.types.FlightStatsKey;
import org.neu.pdpmr.tasks.types.FlightStatsValue;

/**
 * @author yuwen
 */
public class FlightStatsReducer extends Reducer<FlightStatsKey, FlightStatsValue, FlightStatsKey, FlightStatsValue> {
	private static final String DATA_PATH = "data/part";
	private static final String TOPN_PATH = "topN/part";

	private AtomicLongMap<String> airlineStats = AtomicLongMap.create();
	private AtomicLongMap<String> destStats = AtomicLongMap.create();
	private int n;
	private MultipleOutputs<FlightStatsKey, FlightStatsValue> multipleOutputs;

	/**
	 * Expects an int "N" to emit top "N" key1's.
	 * @param context used to read params from context.
	 */
	@Override
	protected void setup(final Context context){
		Configuration conf = context.getConfiguration();
		this.n = conf.getInt("N", 5);
		this.multipleOutputs = new MultipleOutputs<>(context);
	}

	/**
	 * Combines the values with sum and stores in memory counts of every "key1Type" and "key1"
	 * total flights. This reducer works on the assumption that Petitioner has assigned all
	 * key combinations of "key1Type", "key1" to same reducer.
	 *
	 * @param key is the part of composite key "key1Type", "key1".
	 * @param values are the values to be aggregated.
	 * @param context writes aggregated records for the keys to the context.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void reduce(FlightStatsKey key,
					   final Iterable<FlightStatsValue> values,
					   final Context context)
			throws IOException, InterruptedException {
		FlightStatsValue res = new FlightStatsValue();
		for (FlightStatsValue value : values) {
			/*
			 * input pair: (<key>,<values>) more specifically, (<A_Airline_Month_Airport>,
			 * <delay,#delays,#flights>) (<D_Airport_Month_Airline>,
			 * <delay,#delays,#flights>)
			 */
			res.delayInPer += value.delayInPer;
			res.delayFreq  += value.delayFreq;
			res.flightCnt  += value.flightCnt;

			// Summarises the Key1 values.
			if (key.key1Type.get() == FlightStatsKey.KeyType.AIRLINE.ordinal())
				airlineStats.addAndGet(key.key1.toString(), value.flightCnt);
			else
				destStats.addAndGet(key.key1.toString(), value.flightCnt);
		}
		res.key2 = key.key2;
		res.yearMonth = key.yearMonth.get();
		multipleOutputs.write(key, res, DATA_PATH);
	}

	/**
	 * Flushes the buffered topN flights and destinations from memory to multipleoutputs stream.
	 * @param context is the system context to write to.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void cleanup(Context context) throws IOException, InterruptedException {
		emitStats(FlightStatsKey.KeyType.AIRLINE, getTopN(this.airlineStats, n));
		emitStats(FlightStatsKey.KeyType.DESTINATION, getTopN(this.destStats, n));
		this.multipleOutputs.close();
	}

	/**
	 * Writes given collection of key1 to the output stream.
	 * @param key1Type is the key1Type.
	 * @param key1 is the key1 to be emited in output.
	 */
	private void emitStats(final FlightStatsKey.KeyType key1Type,
						   final Collection<String> key1){
		key1.forEach(k1 -> {
			try {
				multipleOutputs.write(new FlightStatsKey(key1Type, k1)
                        , new FlightStatsValue(0, -1, 0)
                        , TOPN_PATH);
			} catch (Exception e) {}
		});
	}

	/**
	 * Sorts the given map on value and returns top n keys.
	 * @param map is the map to find to n.
	 * @param n is the top n entries to get.
	 * @return Top n entries with highest value. May return less than n if given map doesn't have enough entries.
	 */
	private Collection<String> getTopN(final AtomicLongMap<String> map,
									  final int n) {
		final List<Map.Entry<String, Long>> list = new ArrayList<>(map.asMap().entrySet());
		list.sort((o1, o2) -> -o1.getValue().compareTo(o2.getValue()));

		final Collection<String> res = new LinkedList<>();
		for(int i=0; i<n && i<list.size(); i++){
			res.add(list.get(i).getKey());
		}
		return res;
	}
}
