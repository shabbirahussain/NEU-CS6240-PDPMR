package com.pdpmr.taskA1.subtasks;

import com.pdpmr.taskA1.Executor;
import com.pdpmr.taskA1.collectors.CounterCombiner;
import com.pdpmr.taskA1.mappers.LetterCounterMapper;

import java.io.IOException;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by shabbirhussain on 9/8/17.
 */
public class LetterScorer {
    private Executor executor;

    public LetterScorer(int maximumPoolSize) {
        this.executor = new Executor(maximumPoolSize);
    }

    /**
     * Gets the character score from the corpus.
     *
     * @param directory is the directory to traverse and read the text.
     * @return Character map of characters and scores.
     * @throws IOException when unable to read the input file.
     * @throws InterruptedException when thread is interrupted.
     */
    public Map<Character, Integer> getScoreFromCorpus(String directory) throws IOException, InterruptedException {
        Map<Character, Long> cntMap = (Map<Character, Long>) this.executor.run(
                directory
                , new LetterCounterMapper()
                , new CounterCombiner());
        return getLetterScoreFromPercentage(cntMap);
    }

    /**
     * Generates a map of scores based on the rules of percentage occurrence specified in the assignement specs.
     *
     * @param cntMap is the input map of count of characters.
     * @return A map of characters and scores.
     */
    private Map<Character, Integer> getLetterScoreFromPercentage(final Map<Character, Long> cntMap) {
        double tot = (double) cntMap.values().stream().mapToLong(i -> i).sum();
        return cntMap.entrySet()
                .stream()
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> {
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
