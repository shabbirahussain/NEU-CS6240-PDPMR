package com.pdpmr.task0.mappers;

import com.google.common.util.concurrent.AtomicLongMap;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by shabbirhussain on 9/8/17.
 */
public class KScorerMapper implements Mapper{
    private int k;
    private String validCharsRegex;
    private Map<Character, Integer> letterScores;

    public KScorerMapper(String validCharsRegex, int kNeighborhood, Map<Character, Integer>  letterScores){
        this.validCharsRegex = validCharsRegex;
        this.k = kNeighborhood;
        this.letterScores = letterScores;
    }

    @Override
    public Object map(final Object input) {
        return map((String) input);
    }

    /**
     * Counts letters in the given text body
     * @return a map of letters and their respective counts
     */
    WordStats map(String text){
        String[] words = text
                .replaceAll(validCharsRegex, " ")
                .trim()
                .toLowerCase()
                .split("\\s+");


        // Compute the score for each word
        int[] scores = new int[words.length];
        for(int i=0; i<words.length; i++){
            scores[i] = getWordScore(words[i]);
        }

        // Compute k neighborhood score
        WordStats wordStats = new WordStats();
        for(int i=0; i<words.length; i++){
            int score = 0;
            for(int j = Math.max(0, i - k); j < i ;j++){
                score += scores[j];
            }
            for(int j = i + 1; j < Math.min(words.length, i + k); j++){
                score += scores[j];
            }
            wordStats.put(words[i], score);
        }
        return wordStats;
    }

    /**
     * Given a string returns the score of it.
     * @param word is the atomic string to be evaluated for score.
     * @return a value of word from the given rules in the assignment.
     */
    private int getWordScore(String word){
        int totScore = 0;
        for(char c : word.toCharArray()){
            totScore += this.letterScores.getOrDefault(c, 0);
        }
        return totScore;
    }

    /**
     * Created by shabbirhussain on 9/8/17.
     */
    public static class WordStats {
        /**
         * Created by shabbirhussain on 9/8/17.
         */
        static class WordStatsModel {
            private int wordScore;
            private int wordCount;

            public WordStatsModel(){
                wordScore = 0;
                wordCount = 0;
            }
            public WordStatsModel(int wordScore, int wordCount){
                this.wordScore = wordScore;
                this.wordCount = wordCount;
            }

            /**
             * @return The mean of the current stats gathered.
             */
            public int getMean(){
                return wordScore / wordCount;
            }
        }
        private Map<String, WordStatsModel> modelMap;

        public WordStats(){
            modelMap = new TreeMap<>();
        }

        /**
         * Adds the score to the word map and also increments the counter for that word.
         * @param key is the key for which value has to be stored.
         * @param value is the value to be stored.
         */
        public Integer put(final String key, final Integer value){
            WordStatsModel model = modelMap.getOrDefault(key, new WordStatsModel());
            model = new WordStatsModel(model.wordScore + value
                    , model.wordCount + 1);
            modelMap.put(key, model);

            return model.getMean();
        }

        /**
         * Joins two word stats objects combining their score and count.
         * @param wordStats object to be combined from.
         */
        public void putAll(final WordStats wordStats){
            wordStats.modelMap.entrySet().forEach(e->{
                WordStatsModel model = modelMap.getOrDefault(e.getKey(), new WordStatsModel());
                model.wordScore += e.getValue().wordScore;
                model.wordCount += e.getValue().wordCount;
                modelMap.put(e.getKey(), model);
            });
        }

        /**
         * Gets the arithmetic mean of the current score.
         * @param key is the key to search for.
         * @return The mean. If key is not found then NaN.
         */
        public Integer get(final String key){
            WordStatsModel model = modelMap.getOrDefault(key, new WordStatsModel());
            return model.wordScore / model.wordCount;
        }

        /**
         * @return The set of keys present in the map in key sorted order.
         */
        public Collection<String> keySet(){
            return this.modelMap.keySet();
        }

        /**
         * @return The set of keys present in the map in key sorted order.
         */
        public Set<Map.Entry<String, Integer>> entrySet() {
            return this.modelMap
                    .entrySet()
                    .stream()
                    .collect(Collectors.toMap(
                            e -> e.getKey(),
                            e -> e.getValue().getMean()))
                    .entrySet();
        }
    }
}



