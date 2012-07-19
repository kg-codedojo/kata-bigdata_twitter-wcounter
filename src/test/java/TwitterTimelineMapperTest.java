import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mrunit.MapDriver;
import org.junit.Test;

public class TwitterTimelineMapperTest {

	private TwitterTimelineMapper buildTwitterTimelineMapper() {
		TwitterTimelineMapper mapper = new TwitterTimelineMapper();

		return mapper;

	}

	private MapDriver<Text, Text, Text, IntWritable> buildMapDriver(){
		MapDriver<Text, Text, Text, IntWritable> mapDriver = new MapDriver<Text, Text, Text, IntWritable>();
		JobConf conf = new JobConf(TwitterTimeline.class);

		conf.set("xmlinput.start", "<text>");
		conf.set("xmlinput.end", "</text>");
		
		mapDriver.setConfiguration(conf);
		return mapDriver;
	}
	@Test
	public void processesEmptyXMLRecord() throws IOException,
			InterruptedException {
		Text value = new Text("");

		buildMapDriver()
				.withMapper(buildTwitterTimelineMapper()).withInputKey(value)
				// no output expected
				.runTest();
	}

	@Test
	public void processesEmptyTextTag() throws IOException,
			InterruptedException {
		Text value = new Text("<text></text>");

		buildMapDriver()
				.withMapper(buildTwitterTimelineMapper()).withInputKey(value)
				// no output expected
				.runTest();
	}

	@Test
	public void processesEmptyInvalidXmlTag() throws IOException,
			InterruptedException {
		Text value = new Text("<t></t>");

		buildMapDriver()
				.withMapper(buildTwitterTimelineMapper()).withInputKey(value)
				// no output expected
				.runTest();
	}

	@Test
	public void processesValidXmlTag() throws IOException, InterruptedException {
		Text value = new Text("<text>message</text>");

		buildMapDriver()
				.withMapper(buildTwitterTimelineMapper()).withInputKey(value)
				.withOutput(new Text("message"), new IntWritable(1)).runTest();
	}
	
	@Test
	public void processesRealXmlTag() throws IOException, InterruptedException {
		Text value = new Text("<text>@kgurnov @SergiTereshenko test. hello coding followers</text>");

		buildMapDriver()
				.withMapper(buildTwitterTimelineMapper()).withInputKey(value)
				.withOutput(new Text("@kgurnov"), new IntWritable(1))
				.withOutput(new Text("@SergiTereshenko"), new IntWritable(1))
				.withOutput(new Text("test"), new IntWritable(1))
				.withOutput(new Text("hello"), new IntWritable(1))
				.withOutput(new Text("coding"), new IntWritable(1))
				.withOutput(new Text("followers"), new IntWritable(1))
				.runTest();
	}
}
