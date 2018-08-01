package Views;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class CountryPartitioner extends Partitioner<CompositeKey, Text>{
	
	// rewrite to partition data using only country instead of the whole composite key
	@Override
	public int getPartition(CompositeKey key, Text value, int numPartitions) {

		return Math.abs(key.getPK().hashCode() & Integer.MAX_VALUE) % numPartitions;
		
	}

}
