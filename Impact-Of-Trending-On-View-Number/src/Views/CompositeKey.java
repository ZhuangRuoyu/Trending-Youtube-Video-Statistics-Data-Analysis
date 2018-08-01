package Views;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.WritableComparable;

public class CompositeKey implements WritableComparable<CompositeKey>{
	public String country;
	public double percentUp;
	
	public CompositeKey() {		
	}
	
	public CompositeKey(String country, double percentUp) {
		super();
		this.set(country,percentUp);
	}
	
	public void set(String country, double percentUp) {
		this.country = (country == null) ? "" : country;
		this.percentUp = percentUp;
	}
	
	public String getPK() {
		return this.country;
	}
	
	public double getSK() {
		return this.percentUp;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(country);
		out.writeDouble(percentUp);
	}
	
	@Override
	public void readFields(DataInput in) throws IOException {
		country = in.readUTF();
		percentUp = in.readDouble();
	}
	
	@Override
	public int compareTo(CompositeKey o) {
		int countryCmp = country.toLowerCase().compareTo(o.country.toLowerCase());
		if (countryCmp != 0) { // compare country first
			return countryCmp;
		} else {
			return Double.compare(percentUp, o.percentUp);
		}
	}

}
