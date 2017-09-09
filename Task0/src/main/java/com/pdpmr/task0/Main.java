package com.pdpmr.task0;

import com.pdpmr.task0.subtasks.KScorer;
import com.pdpmr.task0.subtasks.LetterScorer;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

public class Main {
    public static void main(String[] args) throws IOException {
        Properties prop = new Properties();
        prop.load(Main.class.getClassLoader().getResourceAsStream("config.properties"));

        // get the property value and print it out
        int kNeighborhood = Integer.parseInt(prop.getProperty("k-neighborhoods"));
        int maxThreads = Integer.parseInt(prop.getProperty("max-threads"));
        String dataDir = prop.getProperty("data-dir");
        String validCharRegex = prop.getProperty("valid-char-regex");
        String validCharRegexK = prop.getProperty("valid-char-regex-k");

        Map<Character, Integer> letterScores;
        {
            long s = System.nanoTime();
            LetterScorer ls = new LetterScorer(maxThreads, validCharRegex);
            letterScores = ls.getScoreFromCorpus(dataDir);
            long e = System.nanoTime();
            System.out.println(letterScores);
            System.out.println((e - s) * Math.pow(10, -6) + " ms");
        }
        {
            long s = System.nanoTime();
            KScorer ks = new KScorer(maxThreads, validCharRegexK, kNeighborhood, letterScores);
            System.out.println(ks.getScoreFromCorpus(dataDir));
            long e = System.nanoTime();
            System.out.println((e - s) * Math.pow(10, -6) + " ms");
        }

    }
}
