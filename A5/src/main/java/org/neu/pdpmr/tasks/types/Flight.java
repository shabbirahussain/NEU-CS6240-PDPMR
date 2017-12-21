package org.neu.pdpmr.tasks.types;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author shabbir.ahussain
 */
public class Flight implements Writable {
    private static final SimpleDateFormat dfDep = new SimpleDateFormat("YYYY, MM, dd, HHmm");
    private static final SimpleDateFormat dfArr = new SimpleDateFormat("HHmm");
    public Text orig, dest, carrier;
    public long depTimeMs, arrTimeMs, delayMs;

    protected Flight(){
        this.orig = new Text();
        this.dest = new Text();
        this.carrier    = new Text();
        this.depTimeMs  = 0;
        this.arrTimeMs  = 0;
        this.delayMs    = 0;
    }

    /**
     * Default constructor.
     * @param orig is the flight origin.
     * @param dest is the flight destination.
     * @param carrier is the carrier code.
     * @param shedDepTimeMs is the depart date time of flight in ms Since midnight, January 1, 1970 UTC.
     * @param shedArrTimeMs is the arrival date time of flight in ms Since midnight, January 1, 1970 UTC.
     * @param actDelayMs is the delay in ms.
     */
    public Flight(final String orig,
                  final String dest,
                  final String carrier,
                  final long shedDepTimeMs,
                  final long shedArrTimeMs,
                  final long actDelayMs){
        this.orig = new Text(orig);
        this.dest = new Text(dest);
        this.carrier    = new Text(carrier);
        this.depTimeMs  = shedDepTimeMs;
        this.arrTimeMs  = shedArrTimeMs;
        this.delayMs    = actDelayMs;
    }
    /**
     * Copy constructor.
     * @param flight is the flight to copy.
     */
    public Flight(final Flight flight){
        this.orig = new Text(flight.orig);
        this.dest = new Text(flight.dest);
        this.carrier    = new Text(flight.carrier);
        this.depTimeMs  = flight.depTimeMs;
        this.arrTimeMs  = flight.arrTimeMs;
        this.delayMs    = flight.delayMs;
    }

    @Override
    public void write(final DataOutput dout) throws IOException {
        this.orig.write(dout);
        this.dest.write(dout);
        this.carrier.write(dout);
        dout.writeLong(this.depTimeMs);
        dout.writeLong(this.arrTimeMs);
        dout.writeLong(this.delayMs);
    }

    @Override
    public void readFields(final DataInput din) throws IOException {
        this.orig.readFields(din);
        this.dest.readFields(din);
        this.carrier.readFields(din);
        this.depTimeMs  = din.readLong();
        this.arrTimeMs  = din.readLong();
        this.delayMs    = din.readLong();
    }

    @Override
    public String toString(){
        return  dfDep.format(new Date(this.depTimeMs)) + "," +
                dfDep.format(new Date(this.arrTimeMs)) + "," +
                this.carrier    + "," +
                this.orig       + "," +
                this.dest ;
    }
}
