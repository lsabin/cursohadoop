import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StoreReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {
	  	  
	  	List<Double> valores = new ArrayList<Double>();

		for (DoubleWritable value : values) {
			Double valor =  value.get();
			valores.add(valor);
			
		}
		
		
		Double maximo = Collections.max(valores);
		
		context.write(key, new DoubleWritable(maximo));

  }
}