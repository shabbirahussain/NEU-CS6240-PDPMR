package com.pdpmr.task0.collectors;

import com.google.common.util.concurrent.AtomicLongMap;

/**
 * Created by shabbirhussain on 9/8/17.
 */
public class CounterCombiner implements Collector{
    AtomicLongMap<Character> map;

    public CounterCombiner(){
        map = AtomicLongMap.create();
    }

    /**
     * Combines the counts from inputs into a new output.
     * @param map1 is input map1.
     * @return AtomicLongMap having keys from both inputs and value equal to the sum of values from both inputs.
     */
    private AtomicLongMap<Character> mergeCount(final AtomicLongMap<Character> map1) {
        map1.asMap().forEach(map::getAndAdd);
        return map;
    }

    @Override
    public Object collect(final Object input) {
        return mergeCount((AtomicLongMap<Character>) input);
    }

    @Override
    public Object getCollectedResult() {
        return map;
    }
}
