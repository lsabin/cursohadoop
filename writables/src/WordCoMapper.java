import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCoMapper extends
Mapper<LongWritable, Text, StringPairWritable, LongWritable> {


	private LongWritable uno = new LongWritable(1);
	private StringPairWritable par = new StringPairWritable();
	private Text palabra1 = new Text();
	private Text palabra2 = new Text();

	@Override
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();
		String[] palabras = line.split("\\W+");
		
		for (int i = 0; i<palabras.length -1 ; i++) {
			
			String w1 = palabras[i].trim();
			String w2 = palabras[i+1].trim();
			
			if (!"".equals(w1) && !"".equals(w2)) {
				palabra1.set(w1);
				palabra2.set(w2);
				
				System.out.println("palabra1: " +  w1 + ", palabra2: " + w2);
				
				par.setValores(palabra1, palabra2);
				context.write(par, uno);
				
			}
			
			
		}

	}
}
