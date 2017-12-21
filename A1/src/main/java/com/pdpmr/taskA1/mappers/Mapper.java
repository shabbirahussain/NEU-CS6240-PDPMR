package com.pdpmr.taskA1.mappers;

import com.pdpmr.taskA1.util.SlidingWindowWordReader;

public interface Mapper {
    /**
     * Transforms input into output
     *
     * @param input is the object given
     * @return An object after transformation
     */
    Object map(final SlidingWindowWordReader input);
}

