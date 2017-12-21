package org.neu.pdpmr.tasks.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.neu.pdpmr.tasks.ml.MLmodel;
import org.neu.pdpmr.tasks.types.Flight;
import org.neu.pdpmr.tasks.types.FlightTuple;

import java.io.*;

/**
 *
 * @author Navya
 */
public class FlightScorer {
    private Configuration conf;
    private MLmodel model;
    private SequenceFile.Reader.Option fileOption;
    private String outPath;

    public FlightScorer(final String trainPath, //"out/results/temp/merged"
                        final String tuplePath,
                        final String outPath) //"out/results/final"
            throws Exception  {
        this.fileOption = SequenceFile.Reader.file(new Path(tuplePath));
        this.model      = new MLmodel(trainPath);
        this.conf       = new Configuration();
        this.outPath    = outPath;

        File file = new File(this.outPath);
        if (file.exists()) file.delete();
        file.mkdirs();
    }

    public void exec() throws Exception {
        try (SequenceFile.Reader reader = new SequenceFile.Reader(conf,fileOption)) {
            Writable key = (NullWritable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (FlightTuple) ReflectionUtils.newInstance(reader.getValueClass(), conf);

            PrintStream out = new PrintStream(this.outPath + "/scoredFlights.csv");

            int score = 0;
            while (reader.next(key, value)) {

                FlightTuple val = (FlightTuple) value;
                Flight leg1     = val.leg1;

                if (val.getLayoverBuffer() - model.predictDelay(leg1) < 0) continue;

                // Write to o/p of likely non flights
                out.println(val.toString());

                // Score the accuracy
                score += (val.getLayoverBuffer() - leg1.delayMs >= 0) ? 1 : -100;
            }
            out.close();

            out = new PrintStream(this.outPath + "/score.txt");
            out.println(score);
            out.close();

        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
