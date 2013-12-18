import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SortMapper extends
Mapper<StringPairWritable, LongWritable, LongWritable, StringPairWritable> {



	@Override
	public void map(StringPairWritable key, LongWritable value, Context context)
			throws IOException, InterruptedException {
		
		context.write(value, key);
			

	}
}
