import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class StockMapper extends Mapper<LongWritable, Text, IdProductoDiaWritable, DiaStockWritable> {


	private IdProductoDiaWritable idDia = new IdProductoDiaWritable();
	private DiaStockWritable diaStock = new DiaStockWritable();
	
	private IntWritable id = new IntWritable();
	private IntWritable dia = new IntWritable();
	private IntWritable entrada = new IntWritable();
	private IntWritable salida = new IntWritable();
	
	
	
	
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    
    String[] valores = line.split("\t");
    
    if (valores != null && valores.length == 4) {
    	id.set(Integer.parseInt(valores[0]));
    	dia.set(Integer.parseInt(valores[1]));
    	salida.set(Integer.parseInt(valores[2]));
    	entrada.set(Integer.parseInt(valores[3]));
    	
    	
    	idDia.setValores(id,dia);
    	diaStock.setValores(dia, entrada, salida);
    	
    	context.write(idDia, diaStock);
    	
    }
    

  }
}