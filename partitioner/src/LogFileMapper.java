import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Example input line:
 * 96.7.4.14 - - [24/Apr/2011:04:20:11 -0400] "GET /cat.jpg HTTP/1.1" 200 12433
 *
 */
public class LogFileMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	
	   private static final String IPADDRESS_PATTERN = 
				"^([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
				"([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
				"([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\." +
				"([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";	
	   
   private Pattern pattern;
   private Matcher matcher;
   
   
  

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
  
	  
	  pattern = Pattern.compile(IPADDRESS_PATTERN);

	  
	  String line = value.toString();
	  
	  String[] valores = line.split("-");
	  
	  //Nos quedamos con el primer valor que es la IP
	  String ip = valores[0].trim();
	  
	  String fecha = valores[2];
	  
	  if(validate(ip)) {
		  
		  //Obtenemos la porcion de fecha
		  String mes = obtieneMesSplit(fecha);
		  
		  
		  if (mes != null && !"".equals(mes)) {
			  context.write(new Text(ip), new Text(mes));
		  }
		  
		  
	  } 

  }
  

  
  private boolean validate(final String ip){

	  
	  matcher = pattern.matcher(ip);
	  return matcher.matches();	    	    
   }  
  
  
  private String obtieneMesSplit(String fecha) {
	  String mes = "";
	  
	  
	  
	  if (!"".equals(fecha)) {
		  String[] campos = fecha.split("/");
		  mes = campos[1];
		  
	  }
	  
	  
	  return mes;
	  
  }
  
  private String obtieneMes(final String linea) {
	  
	  	String mes = "";
	  	String patron = "[(.*?)]";
	  	
	  	
	  	Pattern pattern = Pattern.compile(patron);
	  	Matcher matcher = pattern.matcher(linea);
	  	
	  	if (matcher.find()) {
	  		
	  		
	  		String fecha = matcher.group(0);
	  		
	  		
	  		if (!"".equals(fecha)) {
	  			String[] campos = fecha.split("/");
	  			mes = campos[1];
	  		}
	  	}
	  		  	
	  	
	  	return mes;
	  
  }
}