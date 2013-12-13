import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.WritableComparable;


public class InversoShortWritable extends ShortWritable {

	@Override
	public int compareTo(ShortWritable o) {
		return (-1) * super.compareTo(o);
	}

}
