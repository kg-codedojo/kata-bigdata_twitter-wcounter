import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.streaming.StreamInputFormat;

import xml.XmlInputFormat;

public class TwitterTimeline {

  public static void main(String[] args) throws IOException {
	 
	// drop output dir to avoid error
    FileUtils.deleteDirectory((new File(args[1])));
		
    if (args.length != 2) {
      System.err.println("Usage: TwitterTimeline <input path> <output path>");
      System.exit(-1);
    }
    
    JobConf conf = new JobConf(TwitterTimeline.class);
    conf.setJobName("TwitterTimeline");

    conf.setInputFormat(StreamInputFormat.class);
    conf.set("stream.recordreader.class", "org.apache.hadoop.streaming.StreamXmlRecordReader");
    conf.set("stream.recordreader.begin", "<text>");
    conf.set("stream.recordreader.end", "</text>"); 


    conf.set("xmlinput.start", "<text>");
    conf.set("xmlinput.end", "</text>");
    
    FileInputFormat.addInputPath(conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf, new Path(args[1]));
    
    conf.setMapperClass(TwitterTimelineMapper.class);
    conf.setReducerClass(TwitterTimelineReducer.class);

    conf.setOutputKeyClass(Text.class);
    conf.setOutputValueClass(IntWritable.class);
    conf.setOutputValueGroupingComparator(IntWritable.Comparator.class);

    JobClient.runJob(conf);
    
	System.out.println("-----------------------------------------------------");
	System.out.println("Output file (part-00000) content: ");
	System.out.println("-----------------------------------------------------");
	for (String line : FileUtils
			.readLines(new File(args[1] + "/part-00000"))) {
		System.out.println(line);
	}
	System.out.println("-----------------------------------------------------");
  }
}
