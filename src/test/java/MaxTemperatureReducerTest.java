import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.ReduceDriver;
import org.junit.Test;


public class MaxTemperatureReducerTest {
	@Test
	public void returnsMaximumIntegerInValues() throws IOException,
	InterruptedException {
	new ReduceDriver<Text, IntWritable, Text, IntWritable>()
	.withReducer(new MaxTemperatureReducer())
	.withInputKey(new Text("1950"))
	.withInputValues(Arrays.asList(new IntWritable(10), new IntWritable(5)))
	.withOutput(new Text("1950"), new IntWritable(10))
	.runTest();
	}

}
