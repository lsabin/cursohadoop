import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class ParTermDoc implements WritableComparable<ParTermDoc>{
	private Text term;
	private Text doc;
	
	public ParTermDoc() {
		this(new Text(""), new Text(""));
	}
	public ParTermDoc(Text term, Text doc) {
		this.term = term;
		this.doc = doc;
	}
	
	public void set(Text term, Text doc){
		this.term = term;
		this.doc = doc;
	}
	
	public Text getDoc() {
		return doc;
	}
	
	public Text getTerm() {
		return term;
	}
	
	@Override
	public String toString() {
		return term + ", " + doc;
	}


	@Override
	public int hashCode() {
		/*final int prime = 31;
		int result = 1;
		result = prime * result + ((doc == null) ? 0 : doc.hashCode());
		result = prime * result + ((term == null) ? 0 : term.hashCode());
		return result;*/
		return toString().hashCode();
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ParTermDoc other = (ParTermDoc) obj;
		if (doc == null) {
			if (other.doc != null)
				return false;
		} else if (!doc.equals(other.doc))
			return false;
		if (term == null) {
			if (other.term != null)
				return false;
		} else if (!term.equals(other.term))
			return false;
		return true;
	}
	@Override
	public void write(DataOutput out) throws IOException {
		doc.write(out);
		term.write(out);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		doc.readFields(in);
		term.readFields(in);		
	}

	@Override
	public int compareTo(ParTermDoc o) {
		/*int cmp = this.getDoc().compareTo(o.getDoc()); 
		if (cmp == 0){
			cmp = this.getTerm().compareTo(o.getTerm());
		}*/
		return toString().compareTo(o.toString());
	}

	
}
