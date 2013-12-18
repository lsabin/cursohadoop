import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReduceJoinMapperDocs extends
		Mapper<LongWritable, Text, Text, Text> {

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();

		for (String word : line.split("\\W+")) {
			if (word.length() > 0) {
				context.write(new Text(word.toUpperCase()), new Text(""));
			}
		}
	}
}