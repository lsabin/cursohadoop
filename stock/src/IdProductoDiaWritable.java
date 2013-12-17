import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;


public class IdProductoDiaWritable implements WritableComparable<IdProductoDiaWritable> {
	
	IntWritable id;
	IntWritable dia;
	
	public IdProductoDiaWritable() {
		id = new IntWritable();
		dia = new  IntWritable();
	}
	
	public void setValores(IntWritable id, IntWritable dia) {
		this.id = id;
		this.dia = dia;
	}

	@Override
	public int compareTo(IdProductoDiaWritable o) {
		
		return this.id.compareTo(o.id);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		id.write(out);
		dia.write(out);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		id.readFields(in);
		dia.readFields(in);
		
	}

	@Override
	public String toString() {
		return "(" + id + "," + dia + ")";
	}

	public IntWritable getId() {
		return id;
	}

	public void setId(IntWritable id) {
		this.id = id;
	}

	public IntWritable getDia() {
		return dia;
	}

	public void setDia(IntWritable dia) {
		this.dia = dia;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((dia == null) ? 0 : dia.hashCode());
		result = prime * result + ((id == null) ? 0 : id.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		IdProductoDiaWritable other = (IdProductoDiaWritable) obj;
		if (dia == null) {
			if (other.dia != null)
				return false;
		} else if (!dia.equals(other.dia))
			return false;
		if (id == null) {
			if (other.id != null)
				return false;
		} else if (!id.equals(other.id))
			return false;
		return true;
	}


	
	
	
	
	
	
	
	
	
	
	

}
