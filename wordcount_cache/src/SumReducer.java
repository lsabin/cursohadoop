import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
  
public class SumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

	
	private Map<String, String> glosario = new HashMap<String, String>();

	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int wordCount = 0;
		
		for (IntWritable value : values) {
			wordCount += value.get();
		}
		
		//Obtiene la defincion de la palabra del glosario
		String definicion = glosario.get(key.toString());
		String mostrarDefinicion = definicion != null ? ":" + definicion : "";
		
		
		
		context.write(new Text(key + mostrarDefinicion), new IntWritable(wordCount));
	}

	@Override
	protected void setup(org.apache.hadoop.mapreduce.Reducer.Context context)
			throws IOException, InterruptedException {
		
		
		//Leer el fichero de glossary de la cache
		
		
//		URI[] listaFicheros = context.getCacheFiles();
//		URI fichero = listaFicheros[0]; //Sólo tenemos en cuenta el primero
//		String ruta = fichero.getPath();
		

		/**
		 * Codigo de prueba para comprobar getCacheFiles
		 */
		URI[] listaFicheros = context.getCacheFiles();
		if (listaFicheros != null) {
			System.out.println("lista de ficheros: " + Arrays.toString(listaFicheros));
		} else {
			System.out.println("context.getCacheFiles(): lista de ficheros vacía ");
		}

		
	    String valorParam = context.getConfiguration().get("parametro");
	    System.out.println("valor parametro: " + valorParam);
		
		Path[] paths = context.getLocalCacheFiles();
		Path ruta = paths[0];
				
		File f = new File(ruta.toString());
		
		System.out.println("Fichero: " + f.toString());
		
		if (f.exists()) {
			leeFicheroGlosario(f);
		}
		
		
	}
	
	private void leeFicheroGlosario(File f) throws IOException {
		FileReader reader = null;
		BufferedReader br = null;
		
		
		try {
			reader = new FileReader(f);
			br = new BufferedReader(reader);
			String linea = "";
			while ((linea = br.readLine()) != null) {
				String[] campos = linea.split("\t");
		
				if (campos != null && campos.length >= 2) {
					glosario.put(campos[0].toLowerCase(), campos[1]);
				}
		
			}
		} catch (Exception e) {
			// TODO: handle exception
		} finally {
			if (br != null) br.close();
		}
		
		System.out.println("Terminos en glosario: " + glosario.size());
			
	}
  
  
  
}