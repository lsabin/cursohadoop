import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ComputeNMapper extends
		Mapper<ParTermDoc, IntWritable, Text, DocTFCount> {
	
	private DocTFCount docTFCount = new DocTFCount();
	private IntWritable count = new IntWritable(1);
	@Override
	protected void map(ParTermDoc key, IntWritable value, Context context)
			throws IOException, InterruptedException {
		docTFCount.set(key.getDoc(), value, count);
		context.write(key.getTerm(), docTFCount);
	}
}
