package org.neu.pdpmr.tasks.types;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author shabbir.ahussain
 */
public class FlightStatsValue implements Writable {
    public float delayInPer;
    public long delayFreq, flightCnt;
    public Text key2;
    public int yearMonth;

    public FlightStatsValue(){
        this.key2 = new Text();
        this.yearMonth  = 0;
        this.delayInPer = 0;
        this.delayFreq  = 0;
        this.flightCnt  = 0;
    }

    /**
     * Default constructor.
     * @param delayInPer is the flight delay in minutes.
     * @param delayFreq is the count of delayed flights.
     * @param flightCnt is the total count of flights.
     */
    public FlightStatsValue(final float delayInPer,
                            final long delayFreq,
                            final long flightCnt){
        this();
        this.delayInPer = delayInPer;
        this.delayFreq = delayFreq;
        this.flightCnt  = flightCnt;
    }

    /**
     * Extended constructor, used to store additional values in case of GroupingComparator is needed.
     * @param delayInPer is the flight delay in minutes.
     * @param delayFreq is the count of delayed flights.
     * @param flightCnt is the total count of flights.
     * @param key2 is the secondary key of the record.
     * @param yearMonth is the month of the record.
     */
    public FlightStatsValue(final float delayInPer,
                            final long delayFreq,
                            final long flightCnt,
                            final String key2,
                            final int yearMonth){
        this(delayInPer, delayFreq, flightCnt);
        this.key2 = new Text(key2);
        this.yearMonth = yearMonth;
    }

    @Override
    public void write(final DataOutput dout) throws IOException {
        this.key2.write(dout);
        dout.writeInt(this.yearMonth);
        dout.writeFloat(this.delayInPer);
        dout.writeLong(this.delayFreq);
        dout.writeLong(this.flightCnt);
    }

    @Override
    public void readFields(final DataInput din) throws IOException {
        this.key2.readFields(din);
        this.yearMonth  = din.readInt();
        this.delayInPer = din.readFloat();
        this.delayFreq  = din.readLong();
        this.flightCnt  = din.readLong();
    }

    @Override
    public String toString(){
        return this.delayInPer + "," + this.delayFreq + "," + this.flightCnt;
    }
}
