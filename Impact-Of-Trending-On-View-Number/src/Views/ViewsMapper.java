package Views;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 *  
 * input record format:
 * 
 * header line:
 * video_id,	trending_date,	title,	channel_title,	category_id,	category,		
 * publish_time,				tags,	views,	likes,	dislikes,	comment_count,	thumbnail_link,	
 * comments_disabled,	ratings_disabled,	video_error_or_removed,	description,	country
 * 
 * for instance:
 * LgVi6y5QIjM,	17.14.11,		...,	inscope21,		24,				Entertainment,	
 * 2017-11-13T17:08:49.000Z,	...,	252786,	35885,	230,		1539,			...,			
 * False,				False,				False,					...,			DE
 * 
 * 
 * output key value pairs for the above input: 
 * DE;LgVi6y5QIjM -> 17.14.11;252786
 *
 */

public class ViewsMapper extends Mapper<Object, Text, Text, Text> {
	private Text countryAndId = new Text();
	private Text viewsDate = new Text();
	
	public void map(Object key, Text value, Context context
	) throws IOException, InterruptedException {
		
		//split the data into array by regex, as there are some commas included in values
		//the regex means include the commas with even number of quotes before it only		
		String[] dataArray = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"); 
		
		//eliminate the header line and incomplete data
		if (dataArray.length < 18 || dataArray[0].equals("video_id")){ return;}
		
		countryAndId.set(dataArray[17] + ";" + dataArray[0]);
		viewsDate.set(dataArray[1] + ";" + dataArray[8]);
		context.write(countryAndId, viewsDate);
		
	}
}
