import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class TFMapper extends
		Mapper<LongWritable, Text, ParTermDoc, IntWritable> {
	private Text fileName = new Text();
	private Text wordTxt = new Text();
	private IntWritable i = new IntWritable(1);
	private ParTermDoc parTermDoc = new ParTermDoc();

	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		FileSplit split = (FileSplit) context.getInputSplit();
		fileName.set(split.getPath().getName().trim());
	}

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();

		for (String word : line.split("\\W+")) {
			if (word.length() > 0) {
				wordTxt.set(word.toUpperCase());
				parTermDoc.set(wordTxt, fileName);
					context.write(parTermDoc, i);
			}
		}
	}
}
