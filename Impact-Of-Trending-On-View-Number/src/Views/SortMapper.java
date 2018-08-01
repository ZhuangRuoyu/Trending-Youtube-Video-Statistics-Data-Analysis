package Views;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Input record format
 * DE;LgVi6y5QIjM -> 13.9748602 (just for example)
 *
 * Output for the above input key valueList
 * (DE, 13.9748602)//composite key -> LgVi6y5QIjM (just for example)
 * 
 */

public class SortMapper extends Mapper<Object, Text, CompositeKey, Text> {		
	
	private Text videoId = new Text();
	private CompositeKey countryAndPct = new CompositeKey();

	public void map(LongWritable key, Text value, Context context
	) throws IOException, InterruptedException {
		
		//split the input to get country code, video id and percent increase
		String[] dataArray = value.toString().trim().split(";|\\s+"); 
		
		countryAndPct.set(dataArray[0], Double.parseDouble(dataArray[2]));
		videoId.set(dataArray[1]);
		context.write(countryAndPct, videoId);		
	}
}

