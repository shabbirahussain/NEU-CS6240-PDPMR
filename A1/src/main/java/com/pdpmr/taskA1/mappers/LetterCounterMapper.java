package com.pdpmr.taskA1.mappers;


import com.pdpmr.taskA1.util.SlidingWindowWordReader;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by shabbirhussain on 9/8/17.
 */
public class LetterCounterMapper implements Mapper {
    /**
     * Counts letters in the given text body
     *
     * @return a map of letters and their respective counts
     */
    @Override
    public Map<Character, Long> map(SlidingWindowWordReader buffer) {
        Map<Character, Long> result = new HashMap<>();

        do{
            for (char c : buffer.get(0).toCharArray()){
                result.put(c, result.getOrDefault(c, 0L) + 1);
            }
            buffer.removeFirst();
        } while(buffer.hasMoreElements());
        
        return result;
    }
}
