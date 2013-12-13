import java.io.IOException;
import java.math.BigDecimal;


import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ProductReducer extends Reducer<Text, DoubleWritable, Text, Text> {

  @Override
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {
	  	  
		BigDecimal suma = BigDecimal.valueOf(0);

		for (DoubleWritable value : values) {
			Double valor =  value.get();
			BigDecimal valorImporte = BigDecimal.valueOf(valor);
			
			suma = suma.add(valorImporte);
			
		}
		
		
		context.write(key, new Text(suma.toPlainString()));

  }
}