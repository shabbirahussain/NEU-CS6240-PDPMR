package org.neu.pdpmr.tasks.mappers;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.neu.pdpmr.tasks.subtasks.LetterScorer;
import org.neu.pdpmr.tasks.types.WordStatsWritable;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;

/**
 * @author shabbir.ahussain
 */
public class KScorerMapper extends Mapper<LongWritable, Text, Text, WordStatsWritable> {
    private int k;
    private String validCharsRegex;
    private Map<Character, Integer> letterScores;

    /**
     * Sets up the mapper with essential basics like validCharsRegex to screenout bad characters in input
     * , kNeighborhood and letterScores.
     */
    @Override
    public void setup(Context context){
        Configuration conf = context.getConfiguration();
        this.validCharsRegex = conf.get("validCharsRegex");
        this.k = Integer.parseInt(conf.get("kNeighborhood"));

        try {
            this.letterScores = new LetterScorer("", conf.get("letterScoresFile"))
                    .readPreGeneratedLetterScores();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
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
        final String[] words = datin
                .toString()
                .toLowerCase()
                .replaceAll(validCharsRegex, " ")
                .split("\\s+");

        int score = 0;
        int i = 0;
        if(words.length == 0) return;

        String strCurr = words[i];
        // Initialize score for zeroth element.
        for(int j = 1; j < k; j++){
            score += getWordScore(getSafeWord(words, j));
        }
        // Process elements between 0 to length - k
        do{
            context.write(new Text(strCurr), new WordStatsWritable(score));

            // Prepare for next word
            score += getWordScore(strCurr);         // Add current word score.
            score += getWordScore(getSafeWord(words, i + k));    // Add new entering word score.

            strCurr = getSafeWord(words, ++i);

            score -= getWordScore(strCurr);         // Subtract new current word score.
            score -= getWordScore(getSafeWord(words, i - k));    // Subtract exiting word from the window.
        }while(i < words.length - k);

        // Process words between length - k and length
        do{
            context.write(new Text(strCurr), new WordStatsWritable(score));

            if(i >= words.length - 1) break;

            // Prepare for next word

            strCurr = getSafeWord(words, ++i);

            score -= getWordScore(strCurr);         // Subtract new current word score.
            score -= getWordScore(getSafeWord(words, i - k));    // Subtract exiting word from the window.
        }while(true);
    }


    /**
     * Gets value of of the array. If the index is out of bounds returns empty string.
     * @param words is the array of to fetch values from.
     * @param index is the index to fetch value from.
     * @return value from the array if index is within bounds.
     */
    private String getSafeWord(final String[] words, int index){
        if (index < 0 || index >= words.length) return "";
        return words[index];

    }
    /**
     * Given a string returns the score of it.
     *
     * @param word is the atomic string to be evaluated for score.
     * @return a value of word from the given rules in the assignment.
     */
    private int getWordScore(final String word) {
        int totScore = 0;
        for (char c : word.toCharArray()) {
            totScore += this.letterScores.getOrDefault(c, 0);
        }
        return totScore;
    }
}



