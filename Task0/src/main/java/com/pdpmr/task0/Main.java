package com.pdpmr.task0;

import com.pdpmr.task0.subtasks.KScorer;
import com.pdpmr.task0.subtasks.LetterScorer;
import org.openjdk.jmh.annotations.*;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 5)
@Measurement(iterations = 100)
@Fork(1)
@State(Scope.Benchmark)
public class Main {
    @Param({"1", "2", "3", "4", "5", "6", "7"})
    public int maxThreads;

    public int kNeighborhood;
    public String dataDir, validCharRegex, outFilePath;

    public Main(){
        try {
            Properties prop = new Properties();
            prop.load(Main.class.getClassLoader().getResourceAsStream("config.properties"));

            maxThreads = Integer.parseInt(prop.getProperty("max-threads"));
            dataDir = prop.getProperty("data-dir");
            validCharRegex = prop.getProperty("valid-char-regex");
            kNeighborhood = Integer.parseInt(prop.getProperty("k-neighborhoods"));
            outFilePath = prop.getProperty("csv-file-path");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        Options opt = new OptionsBuilder()
                .include(Main.class.getSimpleName())
                .build();

        new Runner(opt).run();
    }

    /**
     * Generates and executes subtask 1 for getting letter scores from corpus.
     * @return A map of characters and their scores.
     * @throws IOException
     */
    @Benchmark
    public void task0()
            throws IOException{
        Map<Character, Integer>  letterScores = (new LetterScorer(maxThreads, validCharRegex))
                .getScoreFromCorpus(dataDir);

        Files.write(Paths.get(outFilePath)
                , (new KScorer(maxThreads, validCharRegex, kNeighborhood, letterScores))
                        .getScoreFromCorpus(dataDir)
                        .stream()
                        .sorted(Comparator.comparing(Map.Entry::getKey))
                        .map(e-> e.getKey() + ", " + e.getValue())
                        .collect(Collectors.joining("\n"))
                        .getBytes());
    }
}
