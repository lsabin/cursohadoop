import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Example input line:
 * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
 *
 */
public class ImageCounterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	private final String GET = "GET";
	private final IntWritable uno = new IntWritable(1);
	private Text textoClave = new Text();
	private String JPG = "jpg";
	private String JPEG = "jpeg";
	private String GIF = "gif";
	private String OTHER = "other"; 
	
	

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  String linea = value.toString();
	  
	  String url = obtieneUrl(linea);
	  String tipo = obtieneTipoFichero(url);
			  
	  textoClave.set(tipo);
	  context.write(textoClave, uno);
	  
	  incrementaContadores(tipo, context);
	  

  }
  
  
  private String obtieneUrl(String linea) {
	  String url = "";
	  
	  if (!"".equals(linea)) {
		  int indice = linea.indexOf("GET");
		  
		  if (indice != -1) {
			  url = linea.substring(indice);
		  }
		  
	  }
	  
	  return url;
  }
  
  private String obtieneTipoFichero(String url) {
	  String tipoFichero = "";
	  
	  if (!"".equals(url)) {
		  int indice = url.indexOf(".");
		  
		  if (indice != -1) {
			  tipoFichero = url.substring(indice+1);
			  
			  
			  //Hay que hacer un split por espacio y quedarnos con la primera parte que contiene la extension
			  String[] partes = tipoFichero.split(" ");
			  
			  if (partes != null) {
				  tipoFichero = partes[0];
			  }
			  
		  }
	  }
	  
	  return tipoFichero;
	  
  }
  
  private String evaluaTipo(String tipo) {
	  String tipoEvaluado = "";
		  
	  if (JPG.equals(tipo) || JPEG.equals(tipo)) {
		tipoEvaluado = JPG;  
	  } else if (GIF.equals(tipo)) {
		  tipoEvaluado = GIF;
	  } else {
		  tipoEvaluado = OTHER;
	  }
	  
	  return tipoEvaluado;
  }
  
  
  private void incrementaContadores(String tipo, Context context) {
	  
	  context.getCounter("ImageCounter", evaluaTipo(tipo)).increment(1);
	  
  }
  
}