package com.pdpmr.task0.reducers;

import com.google.common.util.concurrent.AtomicLongMap;

/**
 * Created by shabbirhussain on 9/8/17.
 */
public abstract class CounterCombiner {
    /**
     * Combines the counts from inputs into a new output.
     * @param map1 is input map1.
     * @param map2 is input map2.
     * @return AtomicLongMap having keys from both inputs and value equal to the sum of values from both inputs.
     */
    public static AtomicLongMap<Character> mergeCount(final AtomicLongMap<Character> map1, final AtomicLongMap<Character> map2) {
        AtomicLongMap<Character> map = AtomicLongMap.create();
        map.putAll(map1.asMap());
        map2.asMap().forEach(map::getAndAdd);
        return map;
    }
}
