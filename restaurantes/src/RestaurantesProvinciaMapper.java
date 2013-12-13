import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class RestaurantesProvinciaMapper 
	extends Mapper<InversoShortWritable, RestauranteWritable, ProvinciaDireccionWritable, Text> {
	
	private ProvinciaDireccionWritable claveSalida = new ProvinciaDireccionWritable();
	private Text nombre = new Text();
	private Text direccion = new Text();
	private String valoracion = "";
	private RestauranteWritable restaurante = new RestauranteWritable();
	private Text nombreValoracion = new Text();
	private Text txProvincia = new Text();
	


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
    	
    	System.out.println("vamos a obtener la provincia de *" + direccionCompuesta + "*");
    	
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
    	
    	System.out.println("Provincia: " + provincia);
    	
    	
    	nombreValoracion.set("[" + nombre + "," + valoracion + "]" );
    	
    	txProvincia.set(provincia);
    	direccion.set(direccionSimple);
    	claveSalida.setValores(txProvincia, direccion);
   
	    context.write(claveSalida, nombreValoracion);
    }
    

  }
  
  
}