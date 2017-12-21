package org.neu.pdpmr.tasks.types;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Holds the model for storing the word scores. It stores the sum of score for each word
 * across corpus and counts of occurrences as well. While retrieving it returns the average.
 * @author shabbir.ahussain
 */
public class WordKScoreTuple implements WritableComparable<WordKScoreTuple> {
    public Text word;
    public int score;

    public WordKScoreTuple() {
        score = 0;
        this.word = new Text();
    }

    public WordKScoreTuple(final String word, final int value) {
        this.word = new Text(word);
        this.score = value;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(score);
        word.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        score = dataInput.readInt();
        word.readFields(dataInput);
    }

    @Override
    public int hashCode() {
        return (word.hashCode() * 163 );
    }

    @Override
    public String toString() {
        return (this.word.toString() + "=" + this.score);
    }

    @Override
    public int compareTo(WordKScoreTuple o) {
        int res = this.word.compareTo(o.word);
        if (res == 0)
            res = ((Integer)this.score).compareTo(o.score);
        return res;
    }
}
