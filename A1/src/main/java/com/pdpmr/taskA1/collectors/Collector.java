package com.pdpmr.taskA1.collectors;

public interface Collector {
    /**
     * Collects the input object to previous results.
     *
     * @param input is the new input to be collected.
     * @return combined collection output.
     */
    Object collect(final Object input);

    /**
     * @return Gives the collected result back.
     */
    Object getCollectedResult();
}
