import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class RestaurantesMapper extends Mapper<LongWritable, Text, InversoShortWritable, RestauranteWritable> {
	
	private InversoShortWritable swValoracion = new InversoShortWritable();
	private Text nombre = new Text();
	private Text direccion = new Text();
	private RestauranteWritable restaurante = new RestauranteWritable();
	private StringBuilder sbDireccion = new StringBuilder();
	private String SEPARADOR = ",";
	private short valoracion = 0;
	


  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    
    if (!"".equals(line)) {
    
	    String[] valores = line.split(",");
		    
	//    0->id
	//    1->nombre
	//    2->direccion
	//    3->(provincia) valoracion
    //	  4 ->valoracion
	    
	    
	    if (valores != null && valores.length >= 4) {
		    nombre.set(valores[1]);
		    
		    sbDireccion.append(valores[2]);
		    
		    if (valores.length == 5) {
		    	valoracion = Short.valueOf(valores[4].trim());
		    	
		    	//Incluye la provincia en la dirección
		    	sbDireccion.append(SEPARADOR).append(valores[3]);
		    	
		    } else {
		    	valoracion = Short.valueOf(valores[3].trim());
		    }
		    
		    swValoracion.set(valoracion);
		    direccion.set(sbDireccion.toString());
		    
		    restaurante.setValores(nombre, direccion);
		    context.write(swValoracion, restaurante);
	    }
	    
	    sbDireccion.delete(0, sbDireccion.length());
    }
    

  }
  
  
}