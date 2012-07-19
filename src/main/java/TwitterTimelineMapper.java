import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import xml.XmlInputFormat;

public class TwitterTimelineMapper extends MapReduceBase

implements Mapper<Text, Text, Text, IntWritable>, Configurable {

	private String startTag;
	private String endTag;
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private Configuration config;

	@Override
	public void configure(JobConf job) {
		super.configure(job);
		startTag = job.get(XmlInputFormat.START_TAG_KEY);
		endTag = job.get(XmlInputFormat.END_TAG_KEY);
	}

	public void map(Text key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {

		String line = key.toString();
		//System.out.println(key);
		if (line != null && line.trim().length() != 0
				&& line.startsWith(startTag) && line.endsWith(endTag)) {
			
			line = line.substring(startTag.length(),
					line.length() - endTag.length());
			
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				String token = tokenizer.nextToken();
				token = token.replaceAll("[\\.\\,\\?\\-]", "");
				word.set(token);
				output.collect(word, one);
			}
		}

	}


	@Override
	public Configuration getConf() {
		return config;
	}

	@Override
	public void setConf(Configuration config) {
		config = config;

	}

}
