import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class AnalysisMapper extends Mapper <LongWritable, Text, Text, Text> {
	Text t = new Text();
	Text v = new Text();
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] entry = value.toString().split(":");
			
			if(entry[0].equalsIgnoreCase("AIRLINE-POSITIVE")){
				t.set(entry[0]);
				v.set(entry[1]);
				context.write(t, v);
			}
			else if(entry[0].equalsIgnoreCase("AIRLINE-NEUTRAL")){
				t.set(entry[0]);
				v.set(entry[1]);
				context.write(t, v);
			}
			else if(entry[0].equalsIgnoreCase("COUNTRY-POSITIVE")){
				t.set(entry[0]);
				v.set(entry[1]);
				context.write(t, v);
			}
			else if(entry[0].equalsIgnoreCase("COUNTRY-NEUTRAL")){
				t.set(entry[0]);
				v.set(entry[1]);
				context.write(t, v);
			}
			
			else if(entry[0].equalsIgnoreCase("NEGATIVEBYAIRLINE")){
				t.set(entry[0]+":"+entry[1]);
				v.set(entry[2]);
				context.write(t, v);
			}
			else if(entry[0].equalsIgnoreCase("NEGATIVEBYCOUNTRY")){
				t.set(entry[0]+":"+entry[1]);
				v.set(entry[2]);
				context.write(t, v);
			}
			
			else if(entry[0].equalsIgnoreCase("IP")){
				t.set(entry[0]);
				v.set(entry[1]);
				context.write(t, v);
			}
			
			
			/*
			if(entry.length==2){
				tag.set(entry[0]);
				count.set(Integer.valueOf(entry[1]));
				context.write(tag, count);
			}*/
			

	}
	
}


