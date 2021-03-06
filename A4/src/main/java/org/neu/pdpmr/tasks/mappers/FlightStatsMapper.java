package org.neu.pdpmr.tasks.mappers;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;
import org.neu.pdpmr.tasks.types.FlightStatsKey;
import org.neu.pdpmr.tasks.types.FlightStatsValue;
import org.neu.pdpmr.tasks.util.CSVFlightDataParser;

/**
 * @author Navya
 */
public class FlightStatsMapper
        extends Mapper<LongWritable, Text, FlightStatsKey, FlightStatsValue> {

    private CSVFlightDataParser parser;
    private int startYear;

    @Override
    protected void setup(Context context){
        Configuration conf = context.getConfiguration();
        this.startYear = conf.getInt("startYear", 1900);
        try {
            this.parser = new CSVFlightDataParser();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Maps text input to letter count.
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

            if (!validateRecord(f))  return;

            int yearMonth   = (int) ((float) f.get("YEAR") * 100 + (float) f.get("MONTH"));
            if (yearMonth < this.startYear*100) return;

            String airline  = (String) f.get("CARRIER");
            String dest     = (String) f.get("DEST");
            float delayInMin  = (float) f.get("ARR_DELAY_NEW");

            // Normalize the delayInMin and change it into percentage
            if((boolean) f.get("CANCELLED")) {
                delayInMin = 4;
            } else {
                delayInMin /= (float) f.get("CRS_ELAPSED_TIME");
            }
            int delayFreq   = (delayInMin > 0)? 1 : 0;

            FlightStatsKey k1 = new FlightStatsKey(FlightStatsKey.KeyType.AIRLINE, airline, dest, yearMonth);
            FlightStatsKey k2 = new FlightStatsKey(FlightStatsKey.KeyType.DESTINATION, dest, airline, yearMonth);
            FlightStatsValue v = new FlightStatsValue(delayInMin, delayFreq, 1);

            // Duplicate data for airlines as major and airports as major for simplified aggregation.
            context.write(k1, v);
            context.write(k2, v);

        } catch (Exception e) {
            context.getCounter("Exception","E").increment(1);
        }
    }

    /**
     * Validates the given field map from the rules specified in task.
     * @param m is the input map of fields and Object.
     * @return True iff it satisfies all the validation constraints.
     * @throws Exception if there is field missing or datatype are not consistent with schema.
     */
    private static boolean validateRecord(Map<String, Object> m)
            throws Exception{
        float crsArrTime        = (float) m.get("CRS_ARR_TIME");
        float crsDeptTime       = (float) m.get("CRS_DEP_TIME");
        float crsElapsedTime    = (float) m.get("CRS_ELAPSED_TIME");
        float actElapsedTime    = (float) m.get("ACTUAL_ELAPSED_TIME");
        float arrDelay          = (float) m.get("ARR_DELAY");
        float arrDelayMin       = (float) m.get("ARR_DELAY_NEW");
        float timeZone          = crsArrTime - crsDeptTime - crsElapsedTime;
        float cancelledCheck    = timeZone - actElapsedTime;
        float timeModulo        = timeZone % 60;

        // CRSArrTime and CRSDepTime should not be zero
        // timeZone = CRSArrTime - CRSDepTime - CRSElapsedTime and timeZone % 60 should be 0
        if (crsArrTime == 0 || crsDeptTime == 0 || timeModulo != 0) return false;

        // AirportID,  AirportSeqID, CityMarketID, StateFips, Wac should be larger than 0
        if ((float) m.get("ORIGIN_AIRPORT_ID") <= 0)                 return false;
        if ((float) m.get("ORIGIN_AIRPORT_SEQ_ID") <= 0)             return false;
        if ((float) m.get("ORIGIN_CITY_MARKET_ID") <= 0)             return false;
        if ((float) m.get("ORIGIN_STATE_FIPS") <= 0)                 return false;
        if ((float) m.get("ORIGIN_WAC") <= 0)                        return false;

        if ((float) m.get("DEST_AIRPORT_ID") <= 0)                   return false;
        if ((float) m.get("DEST_AIRPORT_SEQ_ID") <= 0)               return false;
        if ((float) m.get("DEST_CITY_MARKET_ID") <= 0)               return false;
        if ((float) m.get("DEST_STATE_FIPS") <= 0)                   return false;
        if ((float) m.get("DEST_WAC") <= 0)                          return false;

        // Origin, Destination,  CityName, State, StateName should not be empty
        if (m.get("ORIGIN").toString().equals(""))                  return false;
        if (m.get("ORIGIN_CITY_NAME").toString().equals(""))        return false;
        if (m.get("ORIGIN_STATE_ABR").toString().equals(""))        return false;
        if (m.get("ORIGIN_STATE_NM").toString().equals(""))         return false;
        if (m.get("DEST").toString().equals(""))                    return false;
        if (m.get("DEST_CITY_NAME").toString().equals(""))          return false;
        if (m.get("DEST_STATE_ABR").toString().equals(""))          return false;
        if (m.get("DEST_STATE_NM").toString().equals(""))           return false;

        // For flights that are not Cancelled: ArrTime -  DepTime - ActualElapsedTime - timeZone should be zero
        if ((boolean)m.get("CANCELLED") && cancelledCheck != 0)     return false;

        // if ArrDelay > 0 then ArrDelay should equal to ArrDelayMinutes if ArrDelay < 0 then ArrDelayMinutes should be zero
        // if ArrDelayMinutes >= 15 then ArrDel15 should be true
        if (arrDelay > 0 && arrDelay != arrDelayMin)                return false;
        if (arrDelay < 0 && arrDelayMin != 0)                       return false;
        if (arrDelayMin >= 15 && !(boolean)m.get("ARR_DEL15"))      return false;

        return true;
    }
}




