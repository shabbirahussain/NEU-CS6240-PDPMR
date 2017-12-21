package org.neu.pdpmr.tasks.types;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author shabbir.ahussain
 */
public class FlightTuple implements Writable {
    public Flight leg1, leg2;
    private long minLayoverMs;

    protected FlightTuple(){
        this.leg1 = new Flight();
        this.leg2 = new Flight();
        this.minLayoverMs = 0;
    }

    /**
     * @return the layover in ms between two flights.
     */
    public long getLayoverBuffer(){
        return leg2.depTimeMs - this.leg1.arrTimeMs - this.minLayoverMs;
    }

    /**
     * Default constructor.
     * @param leg1 is the first flight.
     * @param leg2 is the second flight.
     * @param minLayoverMs is the minimum layover in ms.
     */
    public FlightTuple(final Flight leg1,
                       final Flight leg2,
                       final long minLayoverMs){
        this.leg1 = new Flight(leg1);
        this.leg2 = new Flight(leg2);
        this.minLayoverMs = minLayoverMs;
    }

    @Override
    public void write(final DataOutput dout) throws IOException {
        this.leg1.write(dout);
        this.leg2.write(dout);
        dout.writeLong(this.minLayoverMs);
    }

    @Override
    public void readFields(final DataInput din) throws IOException {
        this.leg1.readFields(din);
        this.leg2.readFields(din);
        this.minLayoverMs = din.readLong();
    }

    @Override
    public String toString(){
        return  "(" + this.leg1 + ")" +
                "(" + this.leg2 + ")";
    }

    public String toDebugString(){
        return  "(" + this.leg1 + ")" +
                "(" + this.leg2 + ")" +
                "(" + this.leg1.delayMs + ")"+
                "(" + this.leg2.depTimeMs + ")"+
                "(" + this.leg1.arrTimeMs + ")"+
                "(" + this.minLayoverMs + ")";
    }
}
