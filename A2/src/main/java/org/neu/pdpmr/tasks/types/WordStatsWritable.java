package org.neu.pdpmr.tasks.types;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Holds the model for storing the word scores. It stores the sum of score for each word
 * across corpus and counts of occurrences as well. While retrieving it returns the average.
 * @author shabbir.ahussain
 */
public class WordStatsWritable implements Writable {
    public long wordScore;
    public long wordCount;

    public WordStatsWritable() {
        wordScore = 0;
        wordCount = 0;
    }

    public WordStatsWritable(final int value) {
        this.add(value);
    }

    /**
     * @return The mean of the current stats gathered.
     */
    public long getMean() {
        return wordScore / wordCount;
    }

    /**
     * Adds the score to the word map and also increments the counter for that word.
     *
     * @param value is the value to be stored.
     */
    public void add(final long value) {
        this.wordScore += value;
        this.wordCount++;
    }

    /**
     * Adds the score to the word map and also increments the counter for that word.
     *
     * @param value is the value to be stored.
     * @param count is the count to be stored.
     */
    public void add(final long value, final long count) {
        this.wordScore += value;
        this.wordCount += count;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(wordScore);
        dataOutput.writeLong(wordCount);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        wordScore = dataInput.readLong();
        wordCount = dataInput.readLong();
    }

    @Override
    public int hashCode() {
        return ((Long)wordScore).hashCode() * 163 + ((Long)wordCount).hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof WordStatsWritable) {
            WordStatsWritable ow = (WordStatsWritable) o;
            return ((Long) this.wordScore).equals(ow.wordScore)
                    && ((Long) this.wordCount).equals(ow.wordCount);
        }
        return false;
    }

    @Override
    public String toString() {
        return ((Long)this.getMean()).toString();
    }
}
