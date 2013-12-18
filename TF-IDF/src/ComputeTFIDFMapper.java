import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class ComputeTFIDFMapper extends
		Mapper<ParTermDoc, TFN, ParTermDoc, DoubleWritable> {
	
	private double N;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		N = context.getConfiguration().getInt("N", 0);
		System.out.println("N=" + N);
	}
	
	@Override
	protected void map(ParTermDoc key, TFN value, Context context)
			throws IOException, InterruptedException {
		
		double idf = Math.log(N / value.getN().get());
		double result = value.getTf().get() * idf;
		System.out.println("key: " + key + " --> " + value + " @ resultado --> " + result);
		context.write(key, new DoubleWritable(result));
	}
}
