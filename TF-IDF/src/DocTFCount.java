import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class DocTFCount implements Writable {
	private Text doc;
	private IntWritable tf;
	private IntWritable count;
	
	public DocTFCount() {
		this(new Text(""), new IntWritable(), new IntWritable());
	}
	
	public DocTFCount(Text doc, IntWritable tf, IntWritable count) {
		this.doc = doc;
		this.tf = tf;
		this.count = count;
	}

	public void set(Text doc, IntWritable tf, IntWritable count) {
		this.doc = doc;
		this.tf = tf;
		this.count = count;
	}

	public IntWritable getCount() {
		return count;
	}
	
	public Text getDoc() {
		return doc;
	}
	
	public IntWritable getTf() {
		return tf;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		doc.write(out);
		tf.write(out);
		count.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		doc.readFields(in);
		tf.readFields(in);
		count.readFields(in);
	}

	@Override
	public String toString() {
		return "(" + doc + ", " + tf + ")";
	}
	
}
