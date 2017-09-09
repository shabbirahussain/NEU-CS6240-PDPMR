package com.pdpmr.task0.mappers;

import com.google.common.util.concurrent.AtomicLongMap;

import java.util.Arrays;

/**
 * Created by shabbirhussain on 9/8/17.
 */
public class LetterCounter implements Mapper{
    private String validCharsRegex;

    public LetterCounter(String validCharsRegex){
        this.validCharsRegex = validCharsRegex;
    }

    @Override
    public Object map(final Object input) {
        return map((String) input);
    }

    /**
     * Counts letters in the given text body
     * @return a map of letters and their respective counts
     */
    AtomicLongMap<Character> map(String text){
        AtomicLongMap<Character> result = AtomicLongMap.create();
        for(char c : text
                .replaceAll(validCharsRegex, "")
                .toLowerCase()
                .toCharArray()){
            result.getAndIncrement(c);
        }
        return result;
    }
}
