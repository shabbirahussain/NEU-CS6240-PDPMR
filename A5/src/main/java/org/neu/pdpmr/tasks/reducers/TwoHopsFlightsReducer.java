package org.neu.pdpmr.tasks.reducers;

import java.io.IOException;

import java.lang.reflect.Type;
import java.util.*;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.neu.pdpmr.tasks.types.Flight;
import org.neu.pdpmr.tasks.types.FlightTuple;
import org.neu.pdpmr.tasks.types.Query;

/**
 * @author shabbir.ahussain
 */
public class TwoHopsFlightsReducer
		extends Reducer<Text, Flight, NullWritable, FlightTuple> {
	private static final long LAYOVER_MIN = 45 * 60 * 1000;
	private static final long LAYOVER_MAX = 12 * 60 * 60 * 1000;
	private static final Type QUERIES_TYP = new TypeToken<Collection<Query>>(){}.getType();
	private Collection<Query> queries;

	/**
	 * Expects an int "N" to emit top "N" key1's.
	 * @param context used to read params from context.
	 */
	@Override
	protected void setup(final Context context){
		Configuration conf = context.getConfiguration();
		try {
			this.queries    = new Gson().fromJson(conf.get("queries"), QUERIES_TYP);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 
	 * Find all the valid two-hop flights which transfers at the airport. These two hop flights 
	 * are related to multiple queries.
	 * 
	 * @param key is a transfer airport 
	 * @param values are all the flights coming to or departing from the same airport. 
	 * 			This airport is specified by the key. 
	 * @param context writes aggregated records for the keys to the context.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void reduce(final Text key,
						  final Iterable<Flight> values,
						  final Context context)
			throws IOException, InterruptedException {
		// 
		List<Flight> allFlights = new LinkedList<>();
		for (Flight f : values) {
			allFlights.add(new Flight(f));
		}

		for(Query q:queries){
			this.emitValidTwoHop(context, allFlights, q);
		}
	}

	/**
	 * Emits all the valid two-hop flights that transfers at the the same airport regarding to a given query
	 * @param context is the context to write in Flight record format.
	 * @param flights is the list of cached flights to generate permutations.
	 * @param q is the current query to match against.
	 */
	private void emitValidTwoHop(final Context context,
								 final Collection<Flight> flights,
								 final Query q){
		//first hop flight (flight fly from the original in the query)
		List<Flight> oFlights = new LinkedList<>();
		//second hop flight (flight fly to the destination in the query)
		List<Flight> dFlights = new LinkedList<>();
		
		// Cache all values for a join.
		for (Flight f : flights) {
			String orig = f.orig.toString();
			String dest = f.dest.toString();

			if (orig.equals(q.orig) && !dest.equals(q.dest)) {
				oFlights.add(new Flight(f));
			} else if (!orig.equals(q.orig) && dest.equals(q.dest)) {
				dFlights.add(new Flight(f));
			}
		}

		// Do a many to many join to spit valid flight combos.
		oFlights.forEach(v1 -> {
			if (q.dateMs != getDateMs(v1.depTimeMs)) return; // Not the current query date.
			dFlights.forEach(v2 -> {
				long layover = v2.depTimeMs - v1.arrTimeMs;
				if (layover >= LAYOVER_MIN && layover <= LAYOVER_MAX)
					try{
						context.write(NullWritable.get(), new FlightTuple(v1, v2, LAYOVER_MIN));
					}catch (Exception e){
						context.getCounter("Exception", "RE").increment(1);
					}
			});
		});
	}

	/**
	 * @param timeMs time in ms to extract date.
	 * @return The time truncated date from the given time.
	 */
	private static long getDateMs(long timeMs){
		Calendar cal = Calendar.getInstance();
		cal.setTimeInMillis(timeMs);
		cal.set(Calendar.HOUR, 0);
		cal.set(Calendar.MINUTE, 0);
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		return cal.getTimeInMillis();
	}
}
