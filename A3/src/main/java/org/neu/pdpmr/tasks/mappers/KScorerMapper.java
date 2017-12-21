package org.neu.pdpmr.tasks.mappers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.neu.pdpmr.tasks.subtasks.LetterScorer;
import org.neu.pdpmr.tasks.types.ScoreFrequencyPair;
import org.neu.pdpmr.tasks.types.SlidingWindowBuffer;
import org.neu.pdpmr.tasks.types.WordKScoreTuple;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.*;

/**
 * @author shabbir.ahussain
 */
public class KScorerMapper
        extends Mapper<LongWritable, Text, WordKScoreTuple, ScoreFrequencyPair> {
    private int k;
    private Map<Character, Integer> letterScores;
    private String validCharsRegex;

    private SlidingWindowBuffer buffer;
    private int score;

    /**
     * Sets up the mapper with essential basics like validCharsRegex to screenout bad characters in input
     * , kNeighborhood and letterScores.
     */
    @Override
    protected void setup(Context context){
        Configuration conf = context.getConfiguration();
        this.k = conf.getInt("kNeighborhood", 3);
        this.validCharsRegex = conf.get("validCharsRegex");
        this.score = 0;

        try {
            this.buffer = new SlidingWindowBuffer(k);
            this.letterScores = new LetterScorer("", conf.get("letterScoresFile"))
                    .readPreGeneratedLetterScores();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Emits the last words in the buffer
     */
    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        buffer.next();
        while(buffer.newCurrWord != null && !buffer.newCurrWord.equals("")){ // Compute last words in the file.
            processWordsInBuffer(context);
            buffer.next();
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
    public void map(LongWritable kin, Text datin, Context context)
            throws IOException, InterruptedException {
        final String[] words = datin
                .toString()
                .toLowerCase()
                .replaceAll(validCharsRegex, " ")
                .trim()
                .split("\\s+");
        if (words.length == 0 || (words.length == 1 && words[0].equals(""))) return;

        buffer.enqueue(Arrays.asList(words));

        while(buffer.hasMoreWords()){                   // Compute full windows.
            buffer.next();
            processWordsInBuffer(context);
        }
    }

    /**
     * Processes words accumulated in the buffer
     * @param context
     */
    private void processWordsInBuffer(Context context)
            throws IOException, InterruptedException{

        // Prepare for next word
        score -= getWordScore(buffer.exitingWord);      // Subtract exiting word from the window.
        score += getWordScore(buffer.currWord);         // Add current word to score.
        score -= getWordScore(buffer.newCurrWord);      // Subtract new current word score.
        score += getWordScore(buffer.enteringWord);     // Add new entering word score.

        context.write(new WordKScoreTuple(buffer.newCurrWord, score),
                new ScoreFrequencyPair(score, 1));
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



