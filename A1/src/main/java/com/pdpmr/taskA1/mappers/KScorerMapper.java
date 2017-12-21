package com.pdpmr.taskA1.mappers;

import com.pdpmr.taskA1.subtasks.KScorer;
import com.pdpmr.taskA1.util.SlidingWindowWordReader;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Created by shabbirhussain on 9/8/17.
 */
public class KScorerMapper implements Mapper {
    private int k;
    private Map<Character, Integer> letterScores;

    public KScorerMapper(int kNeighborhood, Map<Character, Integer> letterScores) {
        this.k = kNeighborhood;
        this.letterScores = letterScores;
    }

    /**
     * Counts letters in the given text body.
     *
     * @return a map of letters and their respective counts
     */
    @Override
    public WordStats map(SlidingWindowWordReader buffer) {
        WordStats wordStats = new WordStats();

        int score = 0;
        int i = 0;

        // Initialize score for zeroth element.
        String strCurr = buffer.get(i);
        for(int j = 1; j < k; j++){
            score += getWordScore(buffer.get(j));
        }

        do{
            wordStats.put(strCurr, score);              // Store score in the map.

            // Prepare for next word
            score += getWordScore(strCurr);             // Add current word score.
            score += getWordScore(buffer.get(i + k));   // Add new entering word score.

            strCurr = buffer.get(++i );                 // Set the new current

            score -= getWordScore(strCurr);             // Subtract new current word score.
            score -= getWordScore(buffer.get(i - k));   // Subtract exiting word from the window.

            if (i == k){
                buffer.removeFirst();
                i --;
            }
        } while(buffer.hasMoreElements());
        return wordStats;
    }


    /**
     * Given a string returns the score of it.
     *
     * @param word is the atomic string to be evaluated for score.
     * @return a value of word from the given rules in the assignment.
     */
    private int getWordScore(final String word) {
        int totScore = 0;
        for (char c : word.toCharArray()) {
            totScore += this.letterScores.getOrDefault(c, 0);
        }
        return totScore;
    }

    /**
     * Created by shabbirhussain on 9/8/17.
     * Holds the model wrappers for storing the word scores. It stores the sum of score for each word
     * across corpus and counts of occurrences as well. While retrieving it returns the average
     * by dividing sum/count.
     */
    public static class WordStats {
        private Map<String, WordStatsModel> modelMap;

        public WordStats() {
            modelMap = new TreeMap<>();
        }

        /**
         * Adds the score to the word map and also increments the counter for that word.
         *
         * @param key   is the key for which value has to be stored.
         * @param value is the value to be stored.
         */
        void put(final String key, final Integer value) {
            if (key.equals("")) return; // Do not add empty words.

            WordStatsModel model = modelMap.getOrDefault(key, new WordStatsModel());
            model = new WordStatsModel(model.wordScore + value
                    , model.wordCount + 1);
            modelMap.put(key, model);

            return;
        }

        /**
         * Joins two word stats objects combining their score and count.
         *
         * @param wordStats object to be combined from.
         */
        public void putAll(final WordStats wordStats) {
            wordStats.modelMap.forEach((k, v) -> {
                WordStatsModel model = this.modelMap.getOrDefault(k, new WordStatsModel());
                model.wordScore += v.wordScore;
                model.wordCount += v.wordCount;
                this.modelMap.put(k, model);
            });
        }

        /**
         * Gets the arithmetic mean of the current score.
         *
         * @param key is the key to search for.
         * @return The mean. If key is not found then NaN.
         */
        public Integer get(final String key) {
            WordStatsModel model = modelMap.getOrDefault(key, new WordStatsModel());
            return model.wordScore / model.wordCount;
        }

        /**
         * @return The set of keys present in the map in key sorted order.
         */
        public Collection<String> keySet() {
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
                            Map.Entry::getKey,
                            e -> e.getValue().getMean()))
                    .entrySet();
        }

        /**
         * Created by shabbirhussain on 9/8/17.
         * Holds the model for storing the word scores. It stores the sum of score for each word
         * across corpus and counts of occurrences as well. While retrieving it returns the average
         * by dividing sum/count.
         */
        static class WordStatsModel {
            private int wordScore;
            private int wordCount;

            WordStatsModel() {
                wordScore = 0;
                wordCount = 0;
            }

            WordStatsModel(int wordScore, int wordCount) {
                this.wordScore = wordScore;
                this.wordCount = wordCount;
            }

            /**
             * @return The mean of the current stats gathered.
             */
            int getMean() {
                return wordScore / wordCount;
            }
        }
    }
}



