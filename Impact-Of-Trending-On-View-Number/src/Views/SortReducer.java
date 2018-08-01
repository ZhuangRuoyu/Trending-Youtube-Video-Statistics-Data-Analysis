package Views;

import java.io.IOException;
import java.text.DecimalFormat;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Input record format
 * (DE, 13.9748602)//composite key -> LgVi6y5QIjM (just for example)
 *
 * Output for the above input key valueList
 * DE; LgVi6y5QIjM -> 1397.5% (just for example)
 * 
 */

public class SortReducer extends Reducer<CompositeKey, Text, Text, Text> {
	Text percentUp = new Text();
	DecimalFormat percentFormat= new DecimalFormat("0.0%"); // formatter for percentage
	
	public void reduce(CompositeKey key, Iterable<Text> value, 
			Context context
	) throws IOException, InterruptedException {	
		Text countryAndId = new Text();
		String percent = percentFormat.format(key.getSK());
	
		// exchange the key and value positions
		percentUp.set(percent);		
		for (Text t:value) {
            countryAndId.set(key.getPK()+"; "+t);
            context.write(countryAndId, percentUp);
        }				
	}	
}
