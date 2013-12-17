import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;


public class DiaStockWritable implements WritableComparable<DiaStockWritable> {
	
	IntWritable dia;
	IntWritable cantidadSalida;
	IntWritable cantidadEntrada;
	
	
	public DiaStockWritable() {
		dia = new  IntWritable();
		cantidadSalida = new  IntWritable();
		cantidadEntrada = new  IntWritable();
	}
	
	public void setValores(IntWritable dia, IntWritable cantidadEntrada, IntWritable cantidadSalida) {
		this.dia = dia;
		this.cantidadEntrada = cantidadEntrada;
		this.cantidadSalida = cantidadSalida;
	}

	@Override
	public int compareTo(DiaStockWritable o) {
		return this.dia.compareTo(o.dia);
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		dia.write(out);
		cantidadEntrada.write(out);
		cantidadSalida.write(out);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		dia.readFields(in);
		cantidadEntrada.readFields(in);
		cantidadSalida.readFields(in);			
		
	}

	@Override
	public String toString() {
		return "(" + dia + "," + cantidadEntrada + ", " + cantidadSalida + ")";
	}

	public IntWritable getDia() {
		return dia;
	}

	public void setDia(IntWritable dia) {
		this.dia = dia;
	}

	public IntWritable getCantidadSalida() {
		return cantidadSalida;
	}

	public void setCantidadSalida(IntWritable cantidadSalida) {
		this.cantidadSalida = cantidadSalida;
	}

	public IntWritable getCantidadEntrada() {
		return cantidadEntrada;
	}

	public void setCantidadEntrada(IntWritable cantidadEntrada) {
		this.cantidadEntrada = cantidadEntrada;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result
				+ ((cantidadEntrada == null) ? 0 : cantidadEntrada.hashCode());
		result = prime * result
				+ ((cantidadSalida == null) ? 0 : cantidadSalida.hashCode());
		result = prime * result + ((dia == null) ? 0 : dia.hashCode());
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
		DiaStockWritable other = (DiaStockWritable) obj;
		if (cantidadEntrada == null) {
			if (other.cantidadEntrada != null)
				return false;
		} else if (!cantidadEntrada.equals(other.cantidadEntrada))
			return false;
		if (cantidadSalida == null) {
			if (other.cantidadSalida != null)
				return false;
		} else if (!cantidadSalida.equals(other.cantidadSalida))
			return false;
		if (dia == null) {
			if (other.dia != null)
				return false;
		} else if (!dia.equals(other.dia))
			return false;
		return true;
	}


	
	

	
	
	
	
	
	
	
	
	
	
	

}
