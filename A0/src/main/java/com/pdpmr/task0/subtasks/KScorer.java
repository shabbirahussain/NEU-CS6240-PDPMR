package com.pdpmr.task0.subtasks;

import com.pdpmr.task0.Executor;
import com.pdpmr.task0.collectors.KScorerCombiner;
import com.pdpmr.task0.mappers.KScorerMapper;

import java.io.IOException;
import java.util.Comparator;
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
     * Gets the word's k-score from the corpus.
     * @param directory is the directory to traverse and read the text.
     * @return A set of words and their scores.
     */
    public Set<Map.Entry<String, Integer>> getScoreFromCorpus(String directory) throws IOException{
        KScorerMapper.WordStats wordStats = (KScorerMapper.WordStats) this.executor.run(
                directory
                , new KScorerMapper(this.validCharsRegex, k, letterScores)
                , new KScorerCombiner());
        return wordStats.entrySet();
    }
}
