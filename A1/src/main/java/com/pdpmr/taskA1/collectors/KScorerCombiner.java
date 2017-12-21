package com.pdpmr.taskA1.collectors;

import com.pdpmr.taskA1.mappers.KScorerMapper;

/**
 * Created by shabbirhussain on 9/8/17.
 */
public class KScorerCombiner implements Collector {
    private KScorerMapper.WordStats wordStats;

    public KScorerCombiner() {
        wordStats = new KScorerMapper.WordStats();
    }

    /**
     * Combines the word stats from input into a new output.
     *
     * @param wordStats1 is input word stats to be combined from.
     * @return AtomicLongMap having keys from both inputs and value equal to the sum of values from both inputs.
     */
    private KScorerMapper.WordStats collect(final KScorerMapper.WordStats wordStats1) {
        wordStats.putAll(wordStats1);
        return wordStats;
    }

    @Override
    public Object collect(final Object input) {
        return collect((KScorerMapper.WordStats) input);
    }

    @Override
    public Object getCollectedResult() {
        return wordStats;
    }
}
