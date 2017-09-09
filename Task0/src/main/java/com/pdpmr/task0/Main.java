package com.pdpmr.task0;

import com.pdpmr.task0.subtasks.KScorer;
import com.pdpmr.task0.subtasks.LetterScorer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) throws IOException {
        Properties prop = new Properties();
        prop.load(Main.class.getClassLoader().getResourceAsStream("config.properties"));

        int maxThreads = Integer.parseInt(prop.getProperty("max-threads"));
        String dataDir = prop.getProperty("data-dir");
        String validCharRegex = prop.getProperty("valid-char-regex");

        Map<Character, Integer> letterScores;
        {
            long s = System.nanoTime();
            letterScores = (new LetterScorer(maxThreads, validCharRegex))
                    .getScoreFromCorpus(dataDir);

            long e = System.nanoTime();
            System.out.println((e - s) * Math.pow(10, -6) + " ms");
        }
        {
            long s = System.nanoTime();

            KScorer ks = new KScorer(maxThreads
                    , validCharRegex
                    , Integer.parseInt(prop.getProperty("k-neighborhoods"))
                    , letterScores);

            Files.write(Paths.get(prop.getProperty("csv-file-path"))
                    , ks.getScoreFromCorpus(dataDir)
                            .stream()
                            .sorted(Comparator.comparing(Map.Entry::getKey))
                            .map(e->e.getKey() + ", " + e.getValue())
                            .collect(Collectors.joining("\n"))
                            .getBytes());

            long e = System.nanoTime();
            System.out.println((e - s) * Math.pow(10, -6) + " ms");
        }

    }
}
