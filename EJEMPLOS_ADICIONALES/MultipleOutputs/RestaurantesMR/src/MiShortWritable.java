import org.apache.hadoop.io.ShortWritable;


public class MiShortWritable extends ShortWritable{
	@Override
	public int compareTo(ShortWritable o) {
		
		return -super.compareTo(o);
	}
}
