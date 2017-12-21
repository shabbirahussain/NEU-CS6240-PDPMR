package org.neu.pdpmr.tasks.types;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author shabbir.ahussain
 */
public class ScoreFrequencyPair implements Writable {
    public int score;
    public long frequency;

    public ScoreFrequencyPair(){}

    public ScoreFrequencyPair(int score, long frequency) {
        this.score = score;
        this.frequency = frequency;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(score);
        dataOutput.writeLong(frequency);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.score = dataInput.readInt();
        this.frequency = dataInput.readLong();
    }

    @Override
    public String toString(){
        return "{score= " + score + ", frequency=" + frequency + "}";
    }
}
