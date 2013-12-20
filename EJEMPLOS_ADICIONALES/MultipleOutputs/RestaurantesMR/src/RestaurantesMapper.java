import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class RestaurantesMapper extends
		Mapper<LongWritable, Text, MiShortWritable, ParNombreDireccion> {
	private static final String SEP = ",";
	//Campos del parseo
	private Text nombreRest = new Text();
	private Text direccion = new Text();
	private StringBuilder direccionRest = new StringBuilder();
	private short valRest;
	//Campos a serializar 
	
	private MiShortWritable valoracion = new MiShortWritable();  
	private ParNombreDireccion par = new ParNombreDireccion();
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();
		if (line.length() > 0) {
			String[] fields = line.split(SEP);
			if (fields.length > 0){
				//fields[0] se ignora para este caso
				nombreRest.set(fields[1]);
				direccionRest.append(fields[2]);
				if (fields.length == 5){			
					direccionRest.append(SEP);
					direccionRest.append(fields[3]);
					valRest = Short.parseShort(fields[4].trim());
				}else{
					valRest = Short.parseShort(fields[3].trim());
				}
				valoracion.set(valRest);
				direccion.set(direccionRest.toString());
				par.set(nombreRest, direccion);
				context.write(valoracion, par);	
			}			
		}
		direccionRest.delete(0, direccionRest.length());
	}
}
