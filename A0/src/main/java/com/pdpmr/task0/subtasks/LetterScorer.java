package com.pdpmr.task0.subtasks;

import com.google.common.util.concurrent.AtomicLongMap;
import com.pdpmr.task0.Executor;
import com.pdpmr.task0.mappers.LetterCounterMapper;
import com.pdpmr.task0.collectors.CounterCombiner;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by shabbirhussain on 9/8/17.
 */
public class LetterScorer {
    Executor executor;
    String validCharsRegex;

    public LetterScorer(int maximumPoolSize, String validCharsRegex){
        this.executor = new Executor(maximumPoolSize);
        this.validCharsRegex = validCharsRegex;
    }

    /**
     * Gets the character score from the corpus.
     * @param directory is the directory to traverse and read the text.
     * @return Character map of characters and scores.
     */
    public Map<Character, Integer> getScoreFromCorpus(String directory) throws IOException{
        AtomicLongMap<Character> cntMap = (AtomicLongMap<Character>) this.executor.run(
                directory
                , new LetterCounterMapper(this.validCharsRegex)
                , new CounterCombiner());
        return getLetterScoreFromPercentage(cntMap.asMap());
    }

    /**
     * Generates a map of scores based on the rules of percentage occurrence specified in the assignement specs.
     * @param cntMap is the input map of count of characters.
     * @return A map of characters and scores.
     */
    private Map<Character, Integer> getLetterScoreFromPercentage(final Map<Character, Long> cntMap){
        double tot =  (double) cntMap.values().stream().mapToLong(i -> i.longValue()).sum();
        return cntMap.entrySet()
                .stream()
                .collect (Collectors.toMap(
                        e-> e.getKey(),
                        e-> {
                            double score = (e.getValue() / tot);
                            if (score >= 0.10) return 0;
                            if (score >= 0.08) return 1;
                            if (score >= 0.06) return 2;
                            if (score >= 0.04) return 4;
                            if (score >= 0.02) return 8;
                            if (score >= 0.01) return 16;
                            return 32;
                        })
                );
    }
}
