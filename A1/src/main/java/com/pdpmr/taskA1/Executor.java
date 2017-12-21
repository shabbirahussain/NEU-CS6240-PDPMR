package com.pdpmr.taskA1;

import com.pdpmr.taskA1.collectors.Collector;
import com.pdpmr.taskA1.mappers.Mapper;
import com.pdpmr.taskA1.util.SlidingWindowWordReader;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Created by shabbirhussain on 9/8/17.
 */
public class Executor {
    private int maxThreads;
    private final String VALID_CHAR_REGEX = "[^a-zA-Z]";

    public Executor(int maximumPoolSize) {
        this.maxThreads = maximumPoolSize;
    }

    /**
     * Gets the list of paths to data files in the given directory.
     *
     * @param directory is the string directory path.
     * @return List of paths to files contained in the directory.
     */
    private List<Path> getListOfFiles(String directory) throws IOException {
        return Files.walk(Paths.get(directory))
                .filter(p -> !p.getFileName().toString().startsWith("."))
                .filter(Files::isRegularFile)
                .collect(Collectors.toList());
    }

    /**
     * Executes a mapper and a collector to return the results.
     *
     * @param directory is the input directory to traverse and read txt files as input.
     * @param mapper    is the mapper to run on the input.
     * @param collector is the collection stage to collect outputs from mappers.
     * @throws IOException when unable to read the input file.
     * @throws InterruptedException when thread is interrupted.
     */
    public Object run(String directory, Mapper mapper, Collector collector) throws IOException, InterruptedException {
        if (this.maxThreads == 1){
            return runSequential(directory, mapper, collector);
        }else {
            return runParallel(directory, mapper, collector);
        }
    }

    /**
     * Executes a mapper and a collector to return the results in parallel manner.
     *
     * @param directory is the input directory to traverse and read txt files as input.
     * @param mapper    is the mapper to run on the input.
     * @param collector is the collection stage to collect outputs from mappers.
     * @throws IOException when unable to read the input file.
     * @throws InterruptedException when thread is interrupted.
     */
    public Object runParallel(String directory, Mapper mapper, Collector collector) throws IOException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(maxThreads);
        ExecutorCompletionService completionService = new ExecutorCompletionService(executorService);

        // Create tasks
        List<Path> files = getListOfFiles(directory);
        files.forEach(p -> completionService.submit((Callable<Object>) () -> {
            long s = System.currentTimeMillis();
            System.out.println("Reading: " + p);
            try {
                return mapper.map(new SlidingWindowWordReader(p, VALID_CHAR_REGEX));
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("Took " + (System.currentTimeMillis() - s)/1000 + " sec.");
            return null;
        }));

        executorService.shutdown();
        executorService.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);

        // Collect results
        for(int i=0; i < files.size(); i++){
            try {
                collector.collect(completionService.take().get());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return collector.getCollectedResult();
    }

    /**
     * Executes a mapper and a collector to return the results in sequential manner.
     *
     * @param directory is the input directory to traverse and read txt files as input.
     * @param mapper    is the mapper to run on the input.
     * @param collector is the collection stage to collect outputs from mappers.
     * @throws IOException when unable to read the input file.
     * @throws InterruptedException when thread is interrupted.
     */
    public Object runSequential(String directory, Mapper mapper, Collector collector) throws IOException, InterruptedException {
        getListOfFiles(directory).forEach(p -> {
            long s = System.currentTimeMillis();
            System.out.println("Reading: " + p);
            try {
                collector.collect(
                        mapper.map(
                                new SlidingWindowWordReader(p, VALID_CHAR_REGEX)));
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("Took " + (System.currentTimeMillis() - s)/1000 + " sec.");
        });

        return collector.getCollectedResult();
    }
}
