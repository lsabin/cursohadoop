import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;


public class TFN implements Writable{
	private IntWritable tf;
	private IntWritable n;
	
	public TFN() {
		this(new IntWritable(), new IntWritable());
	}
	
	public TFN(IntWritable tf, IntWritable n) {
		super();
		this.tf = tf;
		this.n = n;
	}
	
	public IntWritable getTf() {
		return tf;
	}
	
	public IntWritable getN() {
		return n;
	}

	public void setN(IntWritable n) {
		this.n = n;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		tf.write(out);
		n.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		tf.readFields(in);
		n.readFields(in);
	}

	public String toString(){
		return "tf=" + tf + ", n=" + n;
	}
	
}
