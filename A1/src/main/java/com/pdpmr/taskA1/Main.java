package com.pdpmr.taskA1;

import com.pdpmr.taskA1.subtasks.KScorer;
import com.pdpmr.taskA1.subtasks.LetterScorer;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

/**
 * Created by shabbirhussain on 9/8/17.
 * This is the main entrypoint for the program.
 */
public class Main {
    public int maxThreads;

    public int kNeighborhood;
    public String dataDir, outFilePath, runResFilePath;

    private int warmupIterations;
    private int measurementIterations;

    public Main() {
        try {
            Properties prop = new Properties();
            prop.load(new FileInputStream("resources/non_packaged/config.properties"));

            dataDir = prop.getProperty("data-dir");
            kNeighborhood = Integer.parseInt(prop.getProperty("k-neighborhoods"));
            outFilePath = prop.getProperty("csv-file-path");
            maxThreads = Integer.parseInt(prop.getProperty("max-num-threads"));

            warmupIterations = Integer.parseInt(prop.getProperty("warmup-iterations"));
            measurementIterations = Integer.parseInt(prop.getProperty("measurement-iterations"));
            runResFilePath = prop.getProperty("run-res-file-path");

            Files.createDirectories(Paths.get(runResFilePath).getParent());
            Files.createDirectories(Paths.get(outFilePath).getParent());

            if(Files.list(Paths.get(dataDir)).count() < 5)
                unCompressInput(dataDir, prop.getProperty("input-zip-name"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * This function uncompresses given zip file to the output directory.
     * @param outdir is the output directory to use.
     * @param zipfile path to the zip file.
     */
    private void unCompressInput(String outdir, String zipfile){
        try {
            ZipInputStream zin = new ZipInputStream(new FileInputStream(zipfile));
            ZipEntry entry;
            while ((entry = zin.getNextEntry()) != null) {
                if( entry.isDirectory() ) continue;

                byte[] buffer = new byte[4086];
                BufferedOutputStream out = new BufferedOutputStream(
                        new FileOutputStream(
                                new File(outdir, entry.getName())));
                int count = -1;
                while ((count = zin.read(buffer)) != -1)
                    out.write(buffer, 0, count);
                out.close();
            }
            zin.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws Exception {
        Main params = new Main();

        long time = 0L;
        PrintStream out = new PrintStream(new File(params.runResFilePath));
        out.println("maxThreads, time, units");
        for (int i = 1; i <= params.maxThreads; i *= 2) {
            System.out.println("\n\nCurrent Threads:" + i);
            System.out.println("\nExecuting Warmup");
            for(int j = 0; j < params.warmupIterations; j++){
                System.out.print("Iteration " + j + ": ");
                time = params.taskA1(i);
                System.out.println(time + " ms");
            }

            System.out.println("\nExecuting Iterations");
            for(int j = 0; j < params.measurementIterations; j++){
                System.out.println("Iteration " + j + ": ");
                time = params.taskA1(i);
                out.println( i + ", " + time + ", ms/op");
                System.out.println("Time taken=" + time + " ms");
            }
        }
        out.close();
    }

    /**
     * Generates and executes subtask 1 for getting letter scores from corpus.
     * @return time taken in milliseconds
     * @throws IOException when unable to read the input file.
     * @throws InterruptedException when thread is interrupted.
     */
    public long taskA1(int maxThreads) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();

        System.out.println("Scoring letters...");
        Map<Character, Integer> letterScores =
                (new LetterScorer(maxThreads)).getScoreFromCorpus(dataDir);

        System.out.println(letterScores);

        System.out.println("Generating K Scores...");
        Files.write(Paths.get(outFilePath)
                , (new KScorer(maxThreads, kNeighborhood, letterScores))
                        .getScoreFromCorpus(dataDir)
                        .stream()
                        .sorted(Comparator.comparing(Map.Entry::getKey))
                        .map(e -> e.getKey() + ", " + e.getValue())
                        .collect(Collectors.joining("\n"))
                        .getBytes());

        return System.currentTimeMillis() - start;
    }
}
