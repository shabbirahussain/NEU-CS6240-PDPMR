package com.pdpmr.task0.mappers;

import com.google.common.util.concurrent.AtomicLongMap;

import java.util.Arrays;

/**
 * Created by shabbirhussain on 9/8/17.
 */
public abstract class LetterCounter {
    /**
     * Counts letters in the given text body
     * @return a map of letters and their respective counts
     */
    public static AtomicLongMap<Character> countLetters(String text){
        AtomicLongMap<Character> result = AtomicLongMap.create();
        for(char c : text
                .replaceAll("[^\\p{IsAlphabetic}]", "")
                .toLowerCase()
                .toCharArray()){
            result.getAndIncrement(c);
        }
        return result;
    }
}
