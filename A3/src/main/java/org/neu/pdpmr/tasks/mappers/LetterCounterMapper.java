package org.neu.pdpmr.tasks.mappers;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;

/**
 * @author shabbir.ahussain
 */

public class LetterCounterMapper
        extends Mapper<LongWritable, Text, IntWritable, LongWritable> {
    private String validCharsRegex;

    /**
     * Sets up the mapper with essential basics like validCharsRegex to screenout bad characters in input
     * , kNeighborhood and letterScores.
     */
    @Override
    public void setup(Context context){
        this.validCharsRegex = context
                .getConfiguration()
                .get("validCharsRegex");
    }

    /**
     * Maps text input to letter count.
     * @param kin is the input key. (unused)
     * @param datin is the chunk of text to process.
     * @param context is the output context.
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    public void map(LongWritable kin, Text datin, Context context)
            throws IOException, InterruptedException {
        final String words = datin
                .toString()
                .toLowerCase()
                .replaceAll(validCharsRegex, "");

        for(char c : words.toCharArray()){
            context.write(new IntWritable(c), new LongWritable(1));
        };
    }
}
