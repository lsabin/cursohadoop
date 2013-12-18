import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class ReduceJoinMapperGlossary extends Mapper<LongWritable, Text, Text, Text> {

   @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	   String[] fields = value.toString().split("\t");
		if (fields.length >= 2) {
			if (fields[0].length() > 1) {
				context.write(new Text(fields[0].toUpperCase()), new Text(fields[1]));
			}
		}
  }
}