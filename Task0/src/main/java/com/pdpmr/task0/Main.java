package com.pdpmr.task0;

import com.pdpmr.task0.subtasks.KScorer;
import com.pdpmr.task0.subtasks.LetterScorer;
import org.openjdk.jmh.annotations.*;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
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
@State(Scope.Thread)
public class Main {
    int maxThreads, kNeighborhood;
    String dataDir, validCharRegex, outFilePath;
    Map<Character, Integer> letterScores;
    
    public Main(){
        try {
            Properties prop = new Properties();
            prop.load(Main.class.getClassLoader().getResourceAsStream("config.properties"));

            maxThreads = Integer.parseInt(prop.getProperty("max-threads"));
            dataDir = prop.getProperty("data-dir");
            validCharRegex = prop.getProperty("valid-char-regex");
            kNeighborhood = Integer.parseInt(prop.getProperty("k-neighborhoods"));
            outFilePath = prop.getProperty("csv-file-path");

            letterScores = subtask1();
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        generateBenchmarkResults();
        //generateSimpleOutput();
    }

    /**
     * Generates one time output.
     * @throws IOException
     */
    public static void generateSimpleOutput() throws IOException {
        Main main = new Main();
        main.subtask1();
        main.subtask2();
    }

    /**
     * Generates a benchmarf of performance.
     * @throws RunnerException
     */
    public static void generateBenchmarkResults() throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(Main.class.getSimpleName())
                .warmupIterations(5)
                .measurementIterations(5)
                .forks(1)
                .build();

        new Runner(opt).run();
    }

    /**
     * Generates and executes subtask 1 for getting letter scores from corpus.
     * @return A map of characters and their scores.
     * @throws IOException
     */
    @Benchmark
    public Map<Character, Integer> subtask1()
            throws IOException{
        return (new LetterScorer(maxThreads, validCharRegex))
                .getScoreFromCorpus(dataDir);
    }

    /**
     * Calculates the K-score for words and prints to a csv file.
     * @throws IOException
     */
    @Benchmark
    public void subtask2()
            throws IOException {

        Files.write(Paths.get(outFilePath)
                , (new KScorer(maxThreads, validCharRegex, kNeighborhood, letterScores))
                        .getScoreFromCorpus(dataDir)
                        .stream()
                        .sorted(Comparator.comparing(Map.Entry::getKey))
                        .map(e->e.getKey() + ", " + e.getValue())
                        .collect(Collectors.joining("\n"))
                        .getBytes());
    }
}
