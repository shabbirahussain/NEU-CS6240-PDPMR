package org.neu.pdpmr.tasks.mappers;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Map;

import com.google.gson.Gson;

import com.google.gson.reflect.TypeToken;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;

import org.neu.pdpmr.tasks.types.Flight;
import org.neu.pdpmr.tasks.types.Query;
import org.neu.pdpmr.tasks.util.CSVFlightDataParser;
import org.neu.pdpmr.tasks.util.DateTimeUtil;

/**
 * @author shabbir.ahussain
 */
public class TwoHopFlightsMapper
        extends Mapper<LongWritable, Text, Text, Flight> {

    private static final long THREE_DAYS  = 3 * 24 * 60 * 60 * 1000;
    private static final Type QUERIES_TYP = new TypeToken<Collection<Query>>(){}.getType();

    private CSVFlightDataParser parser;
    private Collection<Query> queries;


    @Override
    protected void setup(Context context){
        Configuration conf = context.getConfiguration();
        try {
            this.parser     = new CSVFlightDataParser();
            this.queries    = new Gson().fromJson(conf.get("queries"), QUERIES_TYP);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Maps text flights data to flights object.
     * @param kin is the input key. (unused)
     * @param datin is the chunk of csv to process.
     * @param context is the output context.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable kin, Text datin, Context context)
            throws IOException, InterruptedException {
        try {
            Map<String, Object> f = this.parser.parseLine(datin.toString());

            int year    = (int) ((float) f.get("YEAR"));
            int month   = (int) ((float) f.get("MONTH"));
            int day     = (int) ((float) f.get("DAY_OF_MONTH"));
            int depTime = (int) ((float) f.get("DEP_TIME"));
            int arrTime = (int) ((float) f.get("ARR_TIME"));
            int arrDelayMs = (int) ((float) f.get("ARR_DELAY_NEW"));
            if ((boolean) f.get("CANCELLED"))
                arrDelayMs = 4 * ((int) ((float) f.get("CRSElapsedTime")));
            arrDelayMs *= 60000;

            long flightDateMs = DateTimeUtil.getTimeInMs(year, month, day, 0);

            long depTimeMs = DateTimeUtil.getTimeInMs(year, month, day, depTime);
            long arrTimeMs = DateTimeUtil.getTimeInMs(year, month, day, arrTime);

            String carrier  = ((String) f.get("CARRIER")).toUpperCase();
            String orig     = ((String) f.get("ORIGIN" )).toUpperCase();
            String dest     = ((String) f.get("DEST"   )).toUpperCase();


            for(Query q:queries){
                this.emitValidRecord(context,
                        flightDateMs,
                        orig,
                        dest,
                        carrier,
                        depTimeMs,
                        arrTimeMs,
                        arrDelayMs,
                        q);
            }
        } catch (Exception e) {
            context.getCounter("Exception","E").increment(1);
        }
    }

    /**
     * Emits a flight details if it belongs to one of the queries.
     * @param context is the context to write in Flight record format.
     * @param flightDateMs is the flight date in ms.
     * @param orig is the origin of current mapper flight.
     * @param dest is the destination of current mapper flight.
     * @param carrier is the carrier of current mapper flight.
     * @param depTimeMs is the departure time of current mapper flight.
     * @param arrTimeMs is the arrival time of current mapper flight.
     * @param arrDelayMs is the arrival delay time of current mapper flight.
     * @param q is the current query to match against.
     * @throws Exception
     */
    private void emitValidRecord(final Context context,
                                 final long flightDateMs,
                                 final String orig,
                                 final String dest,
                                 final String carrier,
                                 final long depTimeMs,
                                 final long arrTimeMs,
                                 final long arrDelayMs,
                                 final Query q)
            throws Exception{

        // flightDate >= journeyDateMs && flightDate <= journeyDateMs + X_DAYS
        context.getCounter("MyMap", "s1").increment(1);
        if (!(flightDateMs >= q.dateMs)) return;
        context.getCounter("MyMap", "s2").increment(1);
        if (!(flightDateMs <= q.dateMs + THREE_DAYS)) return;
        context.getCounter("MyMap", "s3").increment(1);

        // Filter and create two legged flights.
        Flight val = new Flight(orig, dest, carrier, depTimeMs, arrTimeMs - arrDelayMs, arrDelayMs);
        if (orig.equals(q.orig)
                && !dest.equals(q.dest)
                && flightDateMs == q.dateMs){
            // candidates with same origin but different destination.
            // (candidate of the first hop use its destination(intermediate destination) as key)
            context.write(new Text(dest), val);
            context.getCounter("MyMap", "s4").increment(1);

        } else if (!orig.equals(q.orig)
                && dest.equals(q.dest)){

            // we ignore the date check condition here to include the second flight that takes off
            // in the next day but has connection time >=45 mins and < 12 hour

            // candidates with different origin but same destination.
            // (candidate of second hop use its origin (intermediate origin) as key)
            context.write(new Text(orig), val);
            context.getCounter("MyMap", "s5").increment(1);
        }
    }
}




