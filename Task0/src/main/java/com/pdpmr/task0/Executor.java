package com.pdpmr.task0;

import com.google.common.util.concurrent.AtomicLongMap;
import com.pdpmr.task0.collectors.Collector;
import com.pdpmr.task0.mappers.Mapper;
import com.pdpmr.task0.collectors.CounterCombiner;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.stream.Collectors;

/**
 * Created by shabbirhussain on 9/8/17.
 */
public class Executor {
    ExecutorService executor;

    public Executor(int maximumPoolSize){
        this.executor = Executors.newFixedThreadPool(maximumPoolSize);
    }

    /**
     * Gets the list of paths to data files in the given directory.
     * @param directory is the string directory path.
     * @return List of paths to files contained in the directory.
     */
    private List<Path> getListOfFiles(String directory) throws IOException{
        return Files.walk(Paths.get(directory))
                    .filter(p -> com.google.common.io.Files.getFileExtension(p.getFileName().toString())
                            .equals("txt"))
                    .collect(Collectors.toList());
    }


    /**
     * Executes a mapper and a collector to return the results.
     * @param directory is the input directory to traverse and read txt files as input.
     * @param mapper is the mapper to run on the input.
     * @param collector is the collection stage to collect outputs from mappers.
     * @return Object which represents the collection of results.
     * @throws IOException
     */
    public Object run(String directory, Mapper mapper, Collector collector) throws IOException{
        List<FutureTask> futureTasks = new ArrayList<>();

        // Create tasks
        getListOfFiles(directory).forEach(p -> {
            futureTasks.add(new FutureTask<>(() -> {
                try {
                    String text = com.google.common.io.Files
                            .asCharSource(p.toFile(), Charset.defaultCharset())
                            .read();
                    return mapper.map(text);
                }catch (IOException e){
                    e.printStackTrace();
                }
                return null;
            }));
        });

        // Execute Tasks
        futureTasks.forEach(executor::execute);
        executor.shutdown();

        // Collect results
        do{
            for (ListIterator<FutureTask> iterator = futureTasks.listIterator(); iterator.hasNext();){
                FutureTask futureTask = iterator.next();
                if (!futureTask.isDone()) continue;

                try {
                    collector.collect(futureTask.get());
                    iterator.remove();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }while(futureTasks.size() > 0);

        return collector.getCollectedResult();
    }
}
