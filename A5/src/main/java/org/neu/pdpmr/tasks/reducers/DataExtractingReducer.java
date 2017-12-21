package org.neu.pdpmr.tasks.reducers;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.io.*;
import org.neu.pdpmr.tasks.util.CSVFlightDataParser;

/**
 * @author yu wen
 */
public class DataExtractingReducer extends Reducer<IntWritable, Text, NullWritable, Text> {
	private String trainDatDir;
	private String queryDatDir;

	// year specified in the query file
	private int queryYear, minTrainYear, maxTrainYear;
	private CSVFlightDataParser parser;
	private Configuration conf;
	private MultipleOutputs<NullWritable, Text> multipleOutputs;

	@Override
	protected void setup(Context context) {
		try {
			conf = context.getConfiguration();
			this.parser = new CSVFlightDataParser();
			this.trainDatDir 	= conf.get("trainDataOutDir");
			this.queryDatDir 	= conf.get("qryYearDataOutDir");

			this.minTrainYear 	= conf.getInt("minTrainYear", 1989);
			this.maxTrainYear 	= conf.getInt("maxTrainYear", 1989);

			this.queryYear = conf.getInt("queryYear", 1989);

			this.multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * Maps text flights data to flights object.
	 *
	 * @param key is the input key. (unused)
	 * @param values is the chunk of csv to process.
	 * @param context is the output context.
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@Override
	protected void reduce(IntWritable key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		for (Text value : values) {
			Map<String, Object> f;
			try {
				f = this.parser.parseLine(value.toString());

				// year in the record
				int recordYear = (int) ((float) f.get("YEAR"));

				// ouput the query year data first
				if (recordYear == this.queryYear || recordYear == this.queryYear+1) {
					this.multipleOutputs.write(NullWritable.get(), value, queryDatDir);
					continue;
				}
				
				// following are steps for extracting training data
				// make sure minTrainYear  <= recordYear <=maxTrainYear
				if (recordYear < minTrainYear || recordYear > maxTrainYear) return;

				// generate labels
				String delay = String.valueOf((int) ((float) f.get("ARR_DELAY_NEW")));
				// delay of a cancelled flight is four times of its scheduled time
				if ((boolean) f.get("CANCELLED"))
					delay = String.valueOf(4 * ((int) ((float) f.get("CRSElapsedTime"))));

				// generate features
				String year = String.valueOf(recordYear);
				String month = String.valueOf((int) ((float) f.get("MONTH")));
				String day = String.valueOf((int) ((float) f.get("DAY_OF_MONTH")));
				String crsDepTime = String.valueOf((int) ((float) f.get("CRS_DEP_TIME")));
				String crsArrTime = String.valueOf((int) ((float) f.get("CRS_ARR_TIME")));
				String carrier = ((String) f.get("CARRIER")).toUpperCase();
				String orig = ((String) f.get("ORIGIN")).toUpperCase();
				String dest = ((String) f.get("DEST")).toUpperCase();

				String csvSplitor = ",";
				String outputInfo = delay + csvSplitor + year + csvSplitor + month + csvSplitor + day + csvSplitor
						+ crsDepTime + csvSplitor + crsArrTime + csvSplitor + carrier + csvSplitor + orig + csvSplitor
						+ dest;

				// only generate 1% of the training data to reduce the size 
				Random rand = new Random(System.currentTimeMillis());
				int max =10000;
				int min =1;
				int res =rand.nextInt(max - min + 1) + min;
				if (res<max/1000) {
					// write the training data
					this.multipleOutputs.write(NullWritable.get(), new Text(outputInfo), trainDatDir);
				}
			} catch (Exception e) {
				context.getCounter("Exception",  "Reducer1").increment(1);
			}
		}
	}
}
