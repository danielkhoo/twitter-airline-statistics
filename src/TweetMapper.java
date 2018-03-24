import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class TweetMapper extends Mapper <LongWritable, Text, Text, Text> {
	Text t = new Text();
	Text v = new Text();
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] col = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
			
			if(col.length==27){//only accept properly formed data
				
				
				//Send Trustpoint
				if(!col[8].isEmpty() && !col[16].isEmpty()){
					t.set("TRUST:"+String.valueOf(col[16]));
					v.set(col[8]);
					context.write(t, v);
				}
				
				//Send Tweets
				if(!col[21].isEmpty()){
					t.set("TWEET");
					v.set(col[21]);
					context.write(t, v);
				}
			}

	}
	
}


