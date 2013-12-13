import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class StoreMapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

	    String line = value.toString();

	    String[] valores = line.split("\t");
	    
	    if (valores.length == 6) {
	    	//Valor 2: Store
	    	String store = valores[2];
	    	
	    	//Valor 4: Venta
	    	String venta = valores[4];
	    	
	    	
	    	//Convertir a Double
	    	Double importe = Double.parseDouble(venta);
	    	
	    	if (store != null && !"".equals(store)) {
	    		context.write(new Text(store), new DoubleWritable(importe));
	    		
	    	}
	    }

  }
}
