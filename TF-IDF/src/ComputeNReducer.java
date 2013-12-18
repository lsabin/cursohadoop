import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ComputeNReducer extends Reducer<Text, DocTFCount, ParTermDoc, TFN> {
	private ParTermDoc parTermDoc = new ParTermDoc();

	@Override
	public void reduce(Text key, Iterable<DocTFCount> values, Context context)
			throws IOException, InterruptedException {
		boolean isThe = false;
		if (key.toString().equals("THE")){
			System.out.println("ENTRADO THE");
			isThe = true;
		}
		int globalCount = 0;
		
		
		//Map para guardar docname: numero tf
		Map<String, Integer> docCount = new HashMap<String, Integer>();
		int sum = 0;
		for (DocTFCount value : values) {
			if (isThe) globalCount++;
			String docName = value.getDoc().toString().trim();
			docCount.put(docName, value.getTf().get());
			sum += value.getCount().get();				
		}
		
		if (isThe) System.out.println("The aparecere: " + globalCount) ;
		for (String doc : docCount.keySet()) {
			TFN tfn = new TFN (new IntWritable(docCount.get(doc).intValue()), new IntWritable(sum));
			parTermDoc.set(key, new Text(doc));
			context.write(parTermDoc, tfn);
		}

	}
}