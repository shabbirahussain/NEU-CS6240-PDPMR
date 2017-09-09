package com.pdpmr.task0;

import com.pdpmr.task0.subtasks.LetterScorer;

import java.io.IOException;
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

        long s = System.nanoTime();
        LetterScorer ls = new LetterScorer(maxThreads, validCharRegex);
        ls.getScoreFromCorpus(dataDir);
        long e = System.nanoTime();

        System.out.println((e - s) * Math.pow(10, -6) + "ms");
        //System.out.println();
    }
}
