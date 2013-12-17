import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class PrimerComparator extends WritableComparator {
	

	public PrimerComparator() {
		
		super(IdProductoDiaWritable.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		IdProductoDiaWritable par1 = (IdProductoDiaWritable) a;
		IdProductoDiaWritable par2 = (IdProductoDiaWritable) b;
		
		
		int cmp = par1.getId().compareTo(par2.getId());
		
		if (cmp == 0) {
			cmp = par1.getDia().compareTo(par2.getDia());
		}
		
		return cmp;
	}
	

}
