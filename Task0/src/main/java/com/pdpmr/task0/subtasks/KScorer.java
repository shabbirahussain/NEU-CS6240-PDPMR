package com.pdpmr.task0.subtasks;

import com.google.common.util.concurrent.AtomicLongMap;
import com.pdpmr.task0.Executor;
import com.pdpmr.task0.collectors.CounterCombiner;
import com.pdpmr.task0.collectors.KScorerCombiner;
import com.pdpmr.task0.mappers.KScorerMapper;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by shabbirhussain on 9/8/17.
 */
public class KScorer {
    Executor executor;
    private int k;
    private String validCharsRegex;
    private Map<Character, Integer> letterScores;

    public KScorer(int maximumPoolSize, String validCharsRegex, int kNeighborhood, Map<Character, Integer>  letterScores){
        this.executor = new Executor(maximumPoolSize);
        this.validCharsRegex = validCharsRegex;
        this.k = kNeighborhood;
        this.letterScores = letterScores;
    }

    /**
     * Gets the character score from the corpus.
     * @param directory is the directory to traverse and read the text.
     * @return A list of words and scores.
     */
    public List<String> getScoreFromCorpus(String directory) throws IOException{
        KScorerMapper.WordStats wordStats = (KScorerMapper.WordStats) this.executor.run(
                directory
                , new KScorerMapper(this.validCharsRegex, k, letterScores)
                , new KScorerCombiner());
        return getListOfScores(wordStats);
    }

    /**
     * Generates a map of scores based on the rules of percentage occurrence specified in the assignement specs.
     * @param wordStats is the input struct from the merged results.
     * @return A list of words and scores.
     */
    private List<String> getListOfScores(final KScorerMapper.WordStats wordStats){
        return wordStats
                .entrySet()
                .stream()
                .map(e->e.getKey() + ", " + e.getValue())
                .collect(Collectors.toList());
    }
}
