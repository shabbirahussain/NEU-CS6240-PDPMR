package org.neu.pdpmr.tasks.mappers;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;

/**
 * @author shabbir.ahussain
 */

public class LetterCounterMapper extends Mapper<LongWritable, Text, IntWritable, LongWritable> {
    private String validCharsRegex;

    /**
     * Sets up the mapper with essential basics like validCharsRegex to screenout bad characters in input.
     */
    @Override
    public void setup(Context context){
        this.validCharsRegex = context.getConfiguration().get("validCharsRegex");
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
    public void map(LongWritable kin, Text datin, Context context) throws IOException, InterruptedException {
        this.map(datin.toString()).forEach((k, v) -> {
            try {
                context.write(new IntWritable(k), new LongWritable(v));
            }catch (Exception e){
                throw new RuntimeException(e);
            }
        });
    }

    /**
     * Counts letters in the given text body.
     *
     * @return a map of letters and their respective counts.
     */
    private Map<Character, Long> map(final String text) {
        Map<Character, Long> result = new HashMap<>();
        for (char c : text
                .replaceAll(validCharsRegex, "")
                .toLowerCase()
                .toCharArray()) {
            result.put(c, result.getOrDefault(c, 0L) + 1);
        }
        return result;
    }
}
