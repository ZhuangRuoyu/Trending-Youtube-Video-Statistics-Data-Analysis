package Views;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ViewsDriver {
	public static void main(String[] args) throws Exception {
		
		// job 1 is to calculate the increased percentage of views for each video 
		// in every country, and filtered out the tuples with increase less than 1000%
		
		Configuration conf1 = new Configuration();		
		Job job1 = Job.getInstance(conf1);
		job1.setNumReduceTasks(3);
		job1.setJarByClass(ViewsDriver.class);
		job1.setJobName("Percent Increase Calculation");
		
		TextInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1] + "/temp"));		
		job1.setMapperClass(ViewsMapper.class);
		job1.setReducerClass(ViewsReducer.class);		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Text.class);
		
		if (!job1.waitForCompletion(true)) {System.exit(1);}
		
		// job 2 is to sort the videos by increase and grouped by country 
		
		Configuration conf2 = new Configuration();			
		Job job2 = Job.getInstance(conf2);
		job2.setNumReduceTasks(3);
		job2.setJarByClass(ViewsDriver.class);
		job2.setJobName("Sort by Percent Increase and Group by Country");
		
		FileInputFormat.setInputPaths(job2, new Path(args[1] + "/temp"));
		FileOutputFormat.setOutputPath(job2, new Path(args[1] + "/final"));	
		//use customized comparator to compare composite key of increase and country
	    job2.setSortComparatorClass(CountryPercentComparator.class);
	    //use customized partitioner to partition only by country
	    job2.setPartitionerClass(CountryPartitioner.class);
		job2.setMapperClass(SortMapper.class);
		job2.setReducerClass(SortReducer.class);		
		job2.setOutputKeyClass(CompositeKey.class);
		job2.setOutputValueClass(Text.class);		

		if (!job2.waitForCompletion(true)) {System.exit(1);}
	}
}

