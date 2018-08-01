package Views;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


/**
 * Input record format:
 * DE;LgVi6y5QIjM -> {17.14.11;252786, 17.15.11;3785435, } (just for example)
 *
 * Output for the above input key valueList:
 * DE;LgVi6y5QIjM -> 13.9748602 (just for example)
 * 
 */

public class ViewsReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	
	DoubleWritable percentUp = new DoubleWritable();	
	
	public void reduce(Text key, Iterable<Text> values, 
			Context context
	) throws IOException, InterruptedException {	
		
		// For each video, initialize variables to store the date and views 
		// of its first and second occurrences;
		Date minDate = new Date(Long.MAX_VALUE);
		Date secondDate = new Date(Long.MAX_VALUE);
		int minViews = 0, secondViews = 0;
		
		for (Text v:values) {	
			
			String[] dataArray = v.toString().split(";");
			
			// For each iterable value, recorded as current date and views.
			// Then compare them to the stored first and second occurrences.
			// If this value is earlier, update the stored data
			try {
				Date currentDate=new SimpleDateFormat("yy.dd.MM").parse(dataArray[0]); 
				int currentViews = Integer.parseInt(dataArray[1]);
				
				if (currentDate.before(minDate)) {
					secondDate = minDate;
					secondViews = minViews;
					minDate = currentDate;
					minViews = currentViews;
				} else if (currentDate.before(secondDate)) {
					secondDate = currentDate;
					secondViews = currentViews;
				}					
			} catch (ParseException e) {
                    System.out.println("Unexcepted date format")
            }			
		}		
		
		// Calculate the percentage increase			
		double percentUpD = (double)secondViews/(double)minViews - 1.0;
		
		// filter out the videos with less than 1000% increase
		// the tuples with only one record will also be filtered out here
		// because they will have a secondViews of 0 and a percentUp of -1
		if (percentUpD<10) {return;}
		
		percentUp.set(percentUpD);
		context.write(key, percentUp);
	}
}
