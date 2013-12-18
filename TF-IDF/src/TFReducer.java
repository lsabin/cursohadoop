import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class TFReducer extends Reducer<ParTermDoc, IntWritable, ParTermDoc, IntWritable> {
	private IntWritable total = new IntWritable();
	@Override
	public void reduce(ParTermDoc key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		
		for (IntWritable value : values) {			
			sum += value.get();
		}
		total.set(sum);
		context.write(key, total);
	}
}