package org.neu.pdpmr.tasks.drivers;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.neu.pdpmr.tasks.mappers.DataExtractingMapper;
import org.neu.pdpmr.tasks.reducers.DataExtractingReducer;

import java.io.IOException;

/**
 * @author yu wen
 */
public class ExtractData {
    private Configuration conf;
    private Path outDir;
    private Path inDir;
    private FileSystem hdfs;
    
    public ExtractData(final String inDir,
                       final String outDir,
                       final String trainDataOutDir,
                       final String qryYearDataOutDir,
                       final int maxYear,
                       final int minYear,
                       final int queryYear)
            throws IOException  {
    		this.inDir = new Path(inDir);
    		this.outDir = new Path(outDir);
        conf = new Configuration();
        conf.setInt("maxTrainYear", maxYear);
        conf.setInt("minTrainYear", minYear);
        conf.setInt("queryYear", queryYear);

        conf.set("trainDataOutDir", trainDataOutDir + "part");
        conf.set("qryYearDataOutDir", qryYearDataOutDir + "part");
        hdfs = FileSystem.newInstance(conf);
    }

    
    public void exec()
            throws IOException, InterruptedException, ClassNotFoundException {
        Job job = Job.getInstance(conf, "training data and query data extractor.");

		job.setJarByClass(ExtractData.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(DataExtractingMapper.class);
		job.setReducerClass(DataExtractingReducer.class);

		// delete existing files
		if (hdfs.exists(this.outDir)) {
			hdfs.delete(this.outDir, true);
		}

		FileInputFormat.addInputPath(job, this.inDir);
		FileInputFormat.setInputDirRecursive(job, true);
		FileOutputFormat.setOutputPath(job, this.outDir);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.waitForCompletion(true);
    }

}
