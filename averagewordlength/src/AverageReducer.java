import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class AverageReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

  @Override
  public void reduce(Text key, Iterable<IntWritable> values, Context context)
      throws IOException, InterruptedException {
	  
		int suma = 0;
		int totalValores = 0;

		for (IntWritable value : values) {
			suma += value.get();
			totalValores ++;
		}
		
		//Calcula la media de las longitudes
		double media = (double) suma / totalValores;
		
		

		context.write(key, new DoubleWritable(media));

  }
}