package com.pdpmr.taskA1.subtasks;

import com.pdpmr.taskA1.Executor;
import com.pdpmr.taskA1.collectors.KScorerCombiner;
import com.pdpmr.taskA1.mappers.KScorerMapper;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * Created by shabbirhussain on 9/8/17.
 */
public class KScorer {
    private Executor executor;
    private int k;
    private Map<Character, Integer> letterScores;

    public KScorer(int maximumPoolSize, int kNeighborhood, Map<Character, Integer> letterScores) {
        this.executor = new Executor(maximumPoolSize);
        this.k = kNeighborhood;
        this.letterScores = letterScores;
    }

    /**
     * Gets the word's k-score from the corpus.
     *
     * @param directory is the directory to traverse and read the text.
     * @return A set of words and their scores.
     * @throws IOException when unable to read the input file.
     * @throws InterruptedException when thread is interrupted.
     */
    public Set<Map.Entry<String, Integer>> getScoreFromCorpus(String directory) throws IOException, InterruptedException {
        KScorerMapper.WordStats wordStats = (KScorerMapper.WordStats) this.executor.run(
                directory
                , new KScorerMapper(k, letterScores)
                , new KScorerCombiner());
        return wordStats.entrySet();
    }
}
