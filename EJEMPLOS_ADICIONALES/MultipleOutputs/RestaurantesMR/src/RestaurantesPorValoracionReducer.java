import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class RestaurantesPorValoracionReducer extends
		Reducer<MiShortWritable, ParNombreDireccion, MiShortWritable, ParNombreDireccion> {

	private MultipleOutputs<MiShortWritable, ParNombreDireccion> multipleOutputs;
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		multipleOutputs = 
				new 
				MultipleOutputs<MiShortWritable, ParNombreDireccion>(context);
	}	
	
	@Override
	protected void cleanup(Context context)
			throws IOException, InterruptedException {
		multipleOutputs.close();
	}
	
	@Override
	protected void reduce(MiShortWritable key, Iterable<ParNombreDireccion> values,
			Context context) throws IOException, InterruptedException {
		for (ParNombreDireccion value : values){	
			if (key.get() > 2){ //Buenos
				multipleOutputs.write(key, value, "recomendados/rest");
			}else{//No ir ni de broma
				multipleOutputs.write(key, value, "no_ir/rest");
			}
		}
	}
}
