import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;


public class ProvinciaDireccionWritable implements WritableComparable<ProvinciaDireccionWritable> {
	
	Text provincia;
	Text direccion;
	
	public ProvinciaDireccionWritable() {
		provincia = new Text("");
		direccion = new Text("");
	}
	
	public void setValores(Text provincia, Text direccion) {
		this.provincia = provincia;
		this.direccion = direccion;
	}

	@Override
	public int compareTo(ProvinciaDireccionWritable o) {
		
		if (!this.provincia.equals(o.provincia)) {
			return this.provincia.compareTo(o.provincia);
		} else {
			return this.direccion.compareTo(o.direccion);
		}
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		provincia.write(out);
		direccion.write(out);		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		provincia.readFields(in);
		direccion.readFields(in);
		
	}

	@Override
	public String toString() {
		return "(" + provincia + "," + direccion + ")";
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((direccion == null) ? 0 : direccion.hashCode());
		result = prime * result
				+ ((provincia == null) ? 0 : provincia.hashCode());
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
		ProvinciaDireccionWritable other = (ProvinciaDireccionWritable) obj;
		if (direccion == null) {
			if (other.direccion != null)
				return false;
		} else if (!direccion.equals(other.direccion))
			return false;
		if (provincia == null) {
			if (other.provincia != null)
				return false;
		} else if (!provincia.equals(other.provincia))
			return false;
		return true;
	}

	public Text getProvincia() {
		return provincia;
	}

	public void setProvincia(Text provincia) {
		this.provincia = provincia;
	}

	public Text getDireccion() {
		return direccion;
	}

	public void setDireccion(Text direccion) {
		this.direccion = direccion;
	}
	
	
	
	
	
	
	
	
	
	
	
	

}
