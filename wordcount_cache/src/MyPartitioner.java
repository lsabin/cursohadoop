import java.util.HashMap;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner<K2, V2> extends Partitioner<Text, IntWritable> implements
    Configurable {

  private Configuration configuration;
  HashMap<String, Integer> months = new HashMap<String, Integer>();

  @Override
  public void setConf(Configuration configuration) {
	  this.configuration = configuration;
  }

  @Override
  public Configuration getConf() {
    return configuration;
  }

  /**
   * You must implement the getPartition method for a partitioner class.
   * This method receives the three-letter abbreviation for the month
   * as its value. (It is the output value from the mapper.)
   * It should return an integer representation of the month.
   * Note that January is represented as 0 rather than 1.
   * 
   * For this partitioner to work, the job configuration must have been
   * set so that there are exactly 12 reducers.
   */
  public int getPartition(Text key, IntWritable value, int numReduceTasks) {

	  int grupo = 0; 
	  int caracter = 0;
	  int numeroPartciones;
	  
	  //Se obtiene la primera letra de la clave
	  char first = key.toString().charAt(0);  
	  
		if (first >= 48 && first <=57) {
			caracter = first + 122 - 47;
		} else {
			caracter = first;
		}
		
	    String valorParam = getConf().get("numero.particiones");
	    
	    if (!"".equals(valorParam)) {
	    	numeroPartciones = Integer.parseInt(valorParam);
	    } else {
	    	numeroPartciones = 4;
	    }
	    System.out.println("parametro numero.particiones : " + numeroPartciones);
		
		
		//Calcula el numero de letras por particion
		int numeroLetrasParticion = 38 / numeroPartciones; // 38 es el numero de letras y numeros
		
		int indice = caracter - 97;
		grupo = indice / numeroLetrasParticion;
	  
	  
	  
	  return grupo;
	  
	  
  }
}
