import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;


public class RestauranteWritable implements Writable {
	private Text nombre;
	private Text direccion;
	
	
	
	public RestauranteWritable() {
		nombre = new Text("");
		direccion = new Text("");
	}
	
	
	public void setValores(Text nombre, Text direccion) {
		this.nombre = nombre;
		this.direccion = direccion;
	}
	
	

	public Text getNombre() {
		return nombre;
	}

	public void setNombre(Text nombre) {
		this.nombre = nombre;
	}

	public Text getDireccion() {
		return direccion;
	}

	public void setDireccion(Text direccion) {
		this.direccion = direccion;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		nombre.write(out);
		direccion.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		nombre.readFields(in);
		direccion.readFields(in);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((direccion == null) ? 0 : direccion.hashCode());
		result = prime * result + ((nombre == null) ? 0 : nombre.hashCode());
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
		RestauranteWritable other = (RestauranteWritable) obj;
		if (direccion == null) {
			if (other.direccion != null)
				return false;
		} else if (!direccion.equals(other.direccion))
			return false;
		if (nombre == null) {
			if (other.nombre != null)
				return false;
		} else if (!nombre.equals(other.nombre))
			return false;
		return true;
	}


	@Override
	public String toString() {
		return nombre + "," + direccion;
	}
	
	
	
	
	

}
