package com.pdpmr.task0.collectors;

public interface Collector {
    /**
     * Collects the input object to previous results.
     * @param input is the new input to be collected.
     * @return combined collection output.
     */
    public Object collect(final Object input);

    /**
     * @return Gives the collected result back.
     */
    public Object getCollectedResult();
}
