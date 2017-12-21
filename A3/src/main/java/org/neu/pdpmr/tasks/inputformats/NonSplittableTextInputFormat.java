package org.neu.pdpmr.tasks.inputformats;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * @author HTDG	Hadoop The Definitive Guide 4th Edition, White, O’Reilly
 */
public class NonSplittableTextInputFormat extends TextInputFormat {
    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }
}