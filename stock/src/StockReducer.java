import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class StockReducer extends Reducer<IdProductoDiaWritable, DiaStockWritable, IntWritable, Text> {
	
	Text textoSalida = new Text("");



  @Override
	public void reduce(IdProductoDiaWritable key, Iterable<DiaStockWritable> values, Context context)
			throws IOException, InterruptedException {
	  
	  	int cantidad = 0;
		

		for (DiaStockWritable value : values) {
			IntWritable claveSalida = key.getId();
			IntWritable diaSalida = key.getDia();
			
			IntWritable entrada= value.getCantidadEntrada();
			IntWritable salida = value.getCantidadSalida();
			int cantidadStock = 0;
			
			cantidadStock = entrada.get() - salida.get();
			
			cantidad += cantidadStock;
			
			textoSalida.set(diaSalida + "\t" + cantidad);
			context.write(claveSalida, textoSalida);
		}
		
	}
}