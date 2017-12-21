package com.pdpmr.taskA1.collectors;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by shabbirhussain on 9/8/17.
 */
public class CounterCombiner implements Collector {
    private Map<Character, Long> map;

    public CounterCombiner() {
        map = new HashMap<>();
    }

    /**
     * Combines the counts from input into a new output.
     *
     * @param map1 is input map to be combined from.
     * @return AtomicLongMap having keys from both inputs and value equal to the sum of values from both inputs.
     */
    private Map<Character, Long> collect(final Map<Character, Long> map1) {
        map1.forEach((k,v) -> {
            this.map.put(k, this.map.getOrDefault(k, 0L) + v);
        });
        return map;
    }

    @Override
    public Object collect(final Object input) {
        return collect((Map<Character, Long>) input);
    }

    @Override
    public Object getCollectedResult() {
        return map;
    }
}
