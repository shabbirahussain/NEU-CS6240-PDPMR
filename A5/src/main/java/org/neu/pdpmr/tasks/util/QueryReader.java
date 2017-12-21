package org.neu.pdpmr.tasks.util;

import org.neu.pdpmr.tasks.types.Query;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author shabbira.hussain
 */
public abstract class QueryReader {
    /**
     * @param queryPath location of the csv query file
     * @return a collection of queries.
     * @throws IOException  unable to read the query file.
     */
    public static Collection<Query> getQueries(String queryPath) throws IOException {
        List<Query> queries = new ArrayList<>();
        BufferedReader br = new BufferedReader(new FileReader(queryPath));
        String line = br.readLine(); // Skip the headers
        while((line = br.readLine())  != null) {
            try {
                queries.add(new Query(line));
            } catch (Exception e){
                System.out.println("Bad Query: \"" + line + "\"");
            }
        }
        return queries;
    }
}
