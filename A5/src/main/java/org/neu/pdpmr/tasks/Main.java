package org.neu.pdpmr.tasks;

import org.apache.commons.cli.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import org.apache.commons.cli.CommandLine;
import org.neu.pdpmr.tasks.drivers.ExtractData;
import org.neu.pdpmr.tasks.drivers.Genrate2HopTuples;
import org.neu.pdpmr.tasks.types.Query;
import org.neu.pdpmr.tasks.util.FlightScorer;
import org.neu.pdpmr.tasks.util.QueryReader;

import java.io.File;
import java.io.IOException;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;

/**
 * @author shabbir.ahussain
 */
public class Main extends Configured implements Tool {
	private static final String OUT_FILE_NAME = "merged";
	private static Options options;

	// Instance members
	private Collection<Query> queries;
	private int maxTrainYear, minTrainYear, queryYear;
	private String inDir, outDir, localOutDir;
	private FileSystem hdfs;

	public Main() throws IOException {
		Configuration conf = new Configuration();
		hdfs = FileSystem.newInstance(conf);
		this.hdfs = FileSystem.newInstance(conf);
	}

	public static void main(String[] args) throws Exception {
		// create Options object
		options = new Options();
		options.addOption("queryFile", true, "location of query file.");
		options.addOption("inDir", true, "Flight history directory.");
		options.addOption("outDir", true, "Path of output directory.");
		options.addOption("localOutDir", true, "Path to local output directory.");

		System.exit(ToolRunner.run(new Configuration(), new Main(), args));
	}

	/**
	 * Parses the commandline options needed for the program.
	 * 
	 * @param args is the list of args to parse.
	 * @return job status.
	 * @throws Exception
	 */
	@Override
	public int run(String[] args) throws Exception {
		System.out.println(Arrays.asList(args));
		CommandLine cmd = new GnuParser().parse(options, args, true);
		try {
			this.queries = QueryReader.getQueries(cmd.getOptionValue("queryFile"));

			Calendar cal = Calendar.getInstance();
			cal.setTimeInMillis(this.queries.iterator().next().dateMs);
			this.queryYear 	  = cal.get(Calendar.YEAR);
			// by default we only extract on year's range as training data 
			this.maxTrainYear = this.queryYear - 1;
			this.minTrainYear = this.queryYear - 1;

			this.inDir 			= cmd.getOptionValue("inDir");
			this.outDir 		= cmd.getOptionValue("outDir");
			this.localOutDir 	= cmd.getOptionValue("localOutDir");
		} catch (Exception e) {
			e.printStackTrace();
			HelpFormatter formatter = new HelpFormatter();
			formatter.printHelp("ant", options);
			return -1;
		}
		taskA5();
		return 0;
	}

	private void taskA5() throws Exception {
		String trainDatDir 		= "temp/training-data/";
		String qryYearDataDir 	= "temp/query-year-data/";

		// extracting query year data and training data.
		new ExtractData(this.inDir, this.outDir, trainDatDir, qryYearDataDir, this.maxTrainYear, this.minTrainYear, this.queryYear).exec();

		// download training data
		getMerged(this.outDir + trainDatDir,
				this.localOutDir + trainDatDir,
				true);
		String trainMerged		= this.localOutDir + "temp/merged";

		// Generate 2 legged flights.
		String ftoutDir = this.outDir + "temp/flight-tuples";
		String ftinDir 	= this.outDir + qryYearDataDir;
		new Genrate2HopTuples(ftinDir, ftoutDir, this.queries).exec();

		ftoutDir += "/part-r-00000";
		String scoreOutDir = this.localOutDir + "final/";
		new FlightScorer(trainMerged, ftoutDir, scoreOutDir).exec();
	}

	/**
	 * Downloads file from HDFS to local.
	 * 
	 * @param hdfsDir HDFS directory path.
	 * @param localDir local direcotry path.
	 * @param merge if set merges all part files into one.
	 * @throws IOException
	 */
	private void getMerged(String hdfsDir, String localDir, boolean merge)
			throws IOException {

		File dir   = new File(localDir);
		if (dir.exists()) {
			Files.walk(dir.toPath())
				.map(Path::toFile)
				.forEach(File::delete);
			dir.delete();
		}

		org.apache.hadoop.fs.Path srcPath = new org.apache.hadoop.fs.Path(hdfsDir);
		hdfs.copyToLocalFile(false,
				new org.apache.hadoop.fs.Path(hdfsDir),
				new org.apache.hadoop.fs.Path(localDir),
				true);

		if (!merge) return;
		OutputStream out = new FileOutputStream(localDir + "../" + OUT_FILE_NAME);

		byte buf[] = new byte[1000];
		for (final File file : dir.listFiles()) {
			if (file.isDirectory()) continue;
			InputStream in = new FileInputStream(file);
			int len = 0;
			while ((len = in.read(buf)) >= 0) {
				out.write(buf, 0, len);
				out.flush();
			}
		}
		out.close();
	}

}
