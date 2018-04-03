/*
 	* Class name: RawDataMapper (Mapper Class 1)
 	* 
 	* Done by: Daniel
 	* 
 	* Description:
 	* This is the mapper for tagging all the values that need counting, 
 	* it sets the relevant flags to redirect inputs to reducers
 	* for all our tasks.
 	* 
 	* 
 	* 
*/
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class RawDataMapper extends Mapper <LongWritable, Text, Text, IntWritable> {
	Text tag = new Text();
	IntWritable one = new IntWritable(1);
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String[] col = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
			
			//This code to try and repair cases where the last 2 cols are missing and at least get the other fields
			if (col.length == 25) {
				String[] tmp = new String[27];
				for (int index = 0; index < col.length; index++) {
					tmp[index] = col[index];
				}
				tmp[25] = " ";
				tmp[26] = " ";
				col = tmp;
			} else if (col.length == 26) {
				String[] tmp = new String[27];
				for (int index = 0; index < col.length; index++) {
					tmp[index] = col[index];
				}
				tmp[26] = " ";
				col = tmp;
			}
			
			if(col.length==27){//only accept properly formed data
				String sentiment = col[14];
				tag.set("SENTIMENT:"+sentiment);
				context.write(tag, one);//Send sentiment pos/neg/neutral
				
				
				if(sentiment.equalsIgnoreCase("negative")){
					//NEGATIVEBYAIRLINE:[airline]:[reason]\t[count]
					if(!col[15].isEmpty() && !col[15].isEmpty()){
						tag.set("NEGATIVEBYAIRLINE:"+String.valueOf(col[16])+":"+String.valueOf(col[15]));
						context.write(tag, one);
					}
					//NEGATIVEBYCOUNTRY:[airline]:[reason]\t[count]
					if(!col[10].isEmpty() && !col[15].isEmpty()){
						tag.set("NEGATIVEBYCOUNTRY:"+String.valueOf(col[10])+":"+String.valueOf(col[15]));
						context.write(tag, one);
					}
				}
				else if (sentiment.equalsIgnoreCase("positive")){
					//AIRLINE-POSITIVE:[airline]\t[count]
					if(!col[16].isEmpty()){
						tag.set("AIRLINE-POSITIVE:"+String.valueOf(col[16]));
						context.write(tag, one);
					}
					if(!col[10].isEmpty()){
						tag.set("COUNTRY-POSITIVE:"+String.valueOf(col[10]));
						context.write(tag, one);
					}
				}
				else if(sentiment.equalsIgnoreCase("neutral")){
					//AIRLINE-NEUTRAL:[airline]\t[count]
					if(!col[16].isEmpty()){
						tag.set("AIRLINE-NEUTRAL:"+String.valueOf(col[16]));
						context.write(tag, one);
					}
					if(!col[10].isEmpty()){
						tag.set("COUNTRY-NEUTRAL:"+String.valueOf(col[10]));
						context.write(tag, one);
					}
				}
				
				
				//Send IPs
				if(!col[13].isEmpty()){
					tag.set("IP:"+String.valueOf(col[13]));
					context.write(tag, one);
				}
				
				
			}

	}
	
}


