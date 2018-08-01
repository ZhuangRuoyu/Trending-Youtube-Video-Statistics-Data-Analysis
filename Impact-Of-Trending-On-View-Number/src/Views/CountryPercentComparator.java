package Views;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CountryPercentComparator extends WritableComparator{
	
	public CountryPercentComparator() {
		super(CompositeKey.class, true);
	}
	
	// rewrite the compare method
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable wc1, WritableComparable wc2) {
		
		CompositeKey key1 = (CompositeKey) wc1;
		CompositeKey key2 = (CompositeKey) wc2;
		
		int countryCmp = key1.country.toLowerCase().compareTo(key2.country.toLowerCase());
		if (countryCmp != 0) {
			return countryCmp; // compare country field first
		} else {
			return -1 * Double.compare(key1.percentUp, key2.percentUp); // then compare percentage
		}
	}

}
