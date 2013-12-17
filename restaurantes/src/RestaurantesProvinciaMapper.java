import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;



public class RestaurantesProvinciaMapper 
	extends Mapper<InversoShortWritable, RestauranteWritable, ProvinciaDireccionWritable, Text> {
	
	private ProvinciaDireccionWritable claveSalida = new ProvinciaDireccionWritable();
	private Text nombre = new Text();
	private Text direccion = new Text();
	private String valoracion = "";
	private RestauranteWritable restaurante = new RestauranteWritable();
	private Text nombreValoracion = new Text();
	private Text txProvincia = new Text();
	
	private MultipleOutputs<ProvinciaDireccionWritable, Text> multipleOutputs;
	
	
	
	


  @Override
  public void map(InversoShortWritable key, RestauranteWritable value, Context context)
      throws IOException, InterruptedException {

    String line = value.toString();
    
    if (!"".equals(line)) {
    	
    	Short val = key.get();
    	
    	valoracion = val.toString();
    	
    	//La direccion y el nombre esta en el valor
    	direccion = value.getDireccion();
    	nombre = value.getNombre();
    	
    	//La provincia esta dentro de la direccion
    	String direccionCompuesta = direccion.toString();
    	String direccionSimple = "";
    	
    	
    	String provincia = "";
    	if (!"".equals(direccionCompuesta)) {
    		int indiceProvincia = direccionCompuesta.indexOf(",");
    		
    		if (indiceProvincia != -1) {
    			provincia = direccionCompuesta.substring(indiceProvincia+1);
    			direccionSimple = direccionCompuesta.substring(0, indiceProvincia);
    		} else {
    			direccionSimple = direccionCompuesta;
    		}
    	}
    	
    	
    	nombreValoracion.set("[" + nombre + "," + valoracion + "]" );
    	
    	txProvincia.set(provincia);
    	direccion.set(direccionSimple);
    	claveSalida.setValores(txProvincia, direccion);
    	
    	short valorValoracion = Short.parseShort(valoracion);
    	
    	if (valorValoracion > 2) {
    		multipleOutputs.write(claveSalida, nombreValoracion,"recomendados/rest");
    	} else {
    		multipleOutputs.write(claveSalida, nombreValoracion,"no_recomendados/rest");
    	}
   
	    //context.write(claveSalida, nombreValoracion);
    }
    

  }


	
	@Override
	protected void cleanup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		multipleOutputs.close();
	
	}
	
	
	
	@Override
	protected void setup(org.apache.hadoop.mapreduce.Mapper.Context context)
			throws IOException, InterruptedException {
		
		
		multipleOutputs = new MultipleOutputs<ProvinciaDireccionWritable, Text>(context);
	}
	  
  
}