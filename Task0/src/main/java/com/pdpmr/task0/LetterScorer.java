package com.pdpmr.task0;

import com.google.common.util.concurrent.AtomicLongMap;
import com.pdpmr.task0.mappers.LetterCounter;
import com.pdpmr.task0.reducers.CounterCombiner;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by shabbirhussain on 9/8/17.
 */
public class LetterScorer {
    int maximumPoolSize;
    public LetterScorer(int maximumPoolSize){
        this.maximumPoolSize = maximumPoolSize;
    }

    /**
     *
     * @param directory
     * @return
     */
    public Map<Character, Integer> getScoreFromCorpus(String directory){
        ExecutorService executor = Executors.newFixedThreadPool(maximumPoolSize);

        List<FutureTask<AtomicLongMap<Character>>> futureTasks = new ArrayList<>();
        try (Stream<Path> paths = Files.walk(Paths.get(directory))) {
            paths
                    .filter(p -> com.google.common.io.Files.getFileExtension(p.getFileName().toString()).equals("txt"))
                    .forEach(p -> {
                        FutureTask<AtomicLongMap<Character>> future = new FutureTask<>(() -> {
                                try {
                                    String text = com.google.common.io.Files.toString(p.toFile(), Charset.defaultCharset());
                                    return LetterCounter.countLetters(text);
                                }catch (IOException e){}
                                return AtomicLongMap.create();
                            });
                        executor.execute(future);
                        futureTasks.add(future);
                    });
        } catch (IOException e) {
            e.printStackTrace();
        }
        executor.shutdown();

        AtomicLongMap<Character> atomicCntMap = AtomicLongMap.create();
        do{
            for (ListIterator<FutureTask<AtomicLongMap<Character>>> iterator = futureTasks.listIterator(); iterator.hasNext();){
                FutureTask<AtomicLongMap<Character>> futureTask = iterator.next();
                if (futureTask.isDone()){
                    try {
                        atomicCntMap = CounterCombiner.mergeCount(atomicCntMap, (AtomicLongMap<Character>) futureTask.get());
                        iterator.remove();
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }
        }while(futureTasks.size() > 0);

        return getLetterScoreFromPercentage(atomicCntMap.asMap());
    }

    /**
     * Generates a map of scores based on the rules of percentage occurrence specified in the assignement specs.
     * @param cntMap is the input map of count of characters.
     * @return A map of characters and scores.
     */
    Map<Character, Integer> getLetterScoreFromPercentage(final Map<Character, Long> cntMap){
        double tot =  (double) cntMap.values().stream().mapToLong(i -> i.longValue()).sum();
        return cntMap.entrySet()
                .stream()
                .collect (Collectors.toMap(
                        e-> e.getKey(),
                        e-> {
                            double score = (e.getValue() / tot);
                            if (score >= 0.10) return 0;
                            if (score >= 0.08) return 1;
                            if (score >= 0.06) return 2;
                            if (score >= 0.04) return 4;
                            if (score >= 0.02) return 8;
                            if (score >= 0.01) return 16;
                            return 32;
                        })
                );
    }

    public static void main(String[] args){
        LetterScorer ls = new LetterScorer(1);
        System.out.println(ls.getScoreFromCorpus("/Users/shabbirhussain/Data/PDPMR/Guttenberg"));
    }
}
