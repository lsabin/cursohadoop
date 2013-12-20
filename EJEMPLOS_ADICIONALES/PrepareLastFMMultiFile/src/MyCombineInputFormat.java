import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

public class MyCombineInputFormat extends
		CombineFileInputFormat<LongWritable, Text> {

	@Override
	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException {
		return new CombineFileRecordReader<LongWritable, Text>(
				(CombineFileSplit) split, context,
				MyMultiFileRecordReader.class);

	}

}

class MyMultiFileRecordReader extends RecordReader<LongWritable, Text> {
	private CombineFileSplit split;
	private TaskAttemptContext context;
	private int index;
	private RecordReader<LongWritable, Text> recordReader;

	public MyMultiFileRecordReader(CombineFileSplit split,
			TaskAttemptContext context, Integer index) {
		this.split = split;
		this.context = context;
		this.index = index;
		recordReader = new LineRecordReader();
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {
		this.split = (CombineFileSplit) split;
		this.context = context;
		if (recordReader == null){
			recordReader = new LineRecordReader();
		}
		FileSplit fileSplit = new FileSplit(this.split.getPath(index),
				this.split.getOffset(index),
				this.split.getLength(index),
				this.split.getLocations());
		recordReader.initialize(fileSplit, this.context);
	}

	public boolean nextKeyValue() throws IOException,InterruptedException {
		return this.recordReader.nextKeyValue();
	}

	@Override
	public LongWritable getCurrentKey() throws IOException, InterruptedException {
		return recordReader.getCurrentKey();
	}

	@Override
	public Text getCurrentValue() throws IOException, InterruptedException {
		return recordReader.getCurrentValue();
	}

	/**
	 * Get the progress within the split
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public float getProgress() throws IOException, InterruptedException {
		return recordReader.getProgress();
	}

	public synchronized void close() throws IOException {
		if (recordReader !=  null){
			recordReader.close();
			recordReader = null;
		}
	}
}
