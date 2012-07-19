import java.io.File;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configured;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class MaxTemperatureDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 2) {
			System.err.printf("Usage: %s [generic options] <input> <output>\n",
					getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		// drop output dir to avoid error
		FileUtils.deleteDirectory((new File(args[1])));

		JobConf conf = new JobConf(MaxTemperature.class);
		conf.setJobName("Max temperature");

		FileInputFormat.addInputPath(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));

		conf.setMapperClass(MaxTemperatureMapper.class);
		conf.setReducerClass(MaxTemperatureReducer.class);

		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		JobClient.runJob(conf);
		return 0;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new MaxTemperatureDriver(), args);
		System.out.println("-----------------------------------------------------");
		System.out.println("Output file (part-00000) content: ");
		System.out.println("-----------------------------------------------------");
		for (String line : FileUtils
				.readLines(new File(args[1] + "/part-00000"))) {
			System.out.println(line);
		}
		System.out.println("-----------------------------------------------------");
		System.exit(exitCode);
	}
}
