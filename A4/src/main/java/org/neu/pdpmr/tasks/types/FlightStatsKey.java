package org.neu.pdpmr.tasks.types;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Holds the composite key for storing the records.
 * Interpretation: {key1Type, key1, key2, yearMonth}
 * Eg.
 *      {'A', "Alaska", "BOS", 201701}
 *      {'D', "BOS", "Alaska", 201701}
 *      {'D', "BOS",,}   // Blank key2 and yearMonth signifies aggregated records.
 * @author shabbir.ahussain
 */
public class FlightStatsKey implements WritableComparable<FlightStatsKey>{
    public enum KeyType {
        AIRLINE, DESTINATION
    };

    public Text key1, key2;
    public IntWritable key1Type, yearMonth;

    protected FlightStatsKey(){
        key1Type  = new IntWritable();
        key1 = new Text();
        key2 = new Text();
        yearMonth = new IntWritable();
    }

    /**
     * Constructor
     * @param key1Type is the type of record this key specifies.
     * @param key1 is the primary key.
     */
    public FlightStatsKey(final KeyType key1Type, final String key1){
        this.key1Type = new IntWritable(key1Type.ordinal());
        this.key1 = new Text(key1);
        this.key2 = new Text();
        this.yearMonth = new IntWritable(-1);
    }

    /**
     * Constructor
     * @param key1Type is the type of record this key specifies.
     * @param key1 is the primary key.
     * @param key2 is the secondary key.
     * @param yearMonth is the integer year month.
     */
    public FlightStatsKey(final KeyType key1Type,
                          final String key1,
                          final String key2,
                          final int yearMonth){
        this.key1Type = new IntWritable(key1Type.ordinal());
        this.key1 = new Text(key1);
        this.key2 = new Text(key2);
        this.yearMonth = new IntWritable(yearMonth);
    }

    /**
     * Constructor
     * @param key1Type is the type of record this key specifies.
     * @param key1 is the primary key.
     * @param key2 is the secondary key.
     * @param yearMonth is the integer year month.
     */
    public FlightStatsKey(final IntWritable key1Type,
                          final Text key1,
                          final Text key2,
                          final int yearMonth){
        this.key1Type = key1Type;
        this.key1 = new Text(key1);
        this.key2 = new Text(key2);
        this.yearMonth = new IntWritable(yearMonth);
    }

    @Override
    public int compareTo(final FlightStatsKey o) {
        int res = 0;
        if ((res = key1Type.compareTo(o.key1Type)) != 0)    return res;
        if ((res = key1.compareTo(o.key1)) != 0)      return res;
        if ((res = key2.compareTo(o.key2)) != 0)      return res;
        if ((res = yearMonth.compareTo(o.yearMonth)) != 0)  return res;
        return res;
    }

    @Override
    public void write(final DataOutput dout) throws IOException {
        key1Type.write(dout);
        key1.write(dout);
        key2.write(dout);
        yearMonth.write(dout);
    }

    @Override
    public void readFields(final DataInput din) throws IOException {
        key1Type.readFields(din);
        key1.readFields(din);
        key2.readFields(din);
        yearMonth.readFields(din);

    }

    @Override
    public int hashCode(){
        return  key1Type.hashCode()     * 1000
                + key1.hashCode()       * 100
                + key2.hashCode()       * 10
                + yearMonth.hashCode();
    }

    @Override
    public String toString(){
        return key1Type.toString() + ", " +
                key1.toString() + ", " +
                key2.toString() + ", " +
                yearMonth.toString();
    }
}
