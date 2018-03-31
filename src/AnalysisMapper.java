import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Hashtable;

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
				t.set("SENTIMENT");
				v.set(entry[0]+":"+entry[1]);
				context.write(t, v);
			}
			else if(entry[0].equalsIgnoreCase("AIRLINE-NEUTRAL")){
				t.set("SENTIMENT");
				v.set(entry[0]+":"+entry[1]);
				context.write(t, v);
			}
			else if(entry[0].equalsIgnoreCase("COUNTRY-POSITIVE")){
				t.set("SENTIMENT");
				v.set(entry[0]+":"+entry[1]);
				context.write(t, v);
			}
			else if(entry[0].equalsIgnoreCase("COUNTRY-NEUTRAL")){
				t.set("SENTIMENT");
				v.set(entry[0]+":"+entry[1]);
				context.write(t, v);
			}
			
			//Change Negative by airline as the sole flag so they all go to the same reducer
			//Form k[entry[0]:entry[1]] v[entry[2]]  to k[entry[0]] v[entry1:entry2]
			
			else if(entry[0].equalsIgnoreCase("NEGATIVEBYAIRLINE")){
				//t.set(entry[0]+":"+entry[1]);
				//v.set(entry[2]);
				t.set(entry[0]);
				v.set(entry[1]+":"+entry[2]);
				context.write(t, v);
			}
			else if(entry[0].equalsIgnoreCase("NEGATIVEBYCOUNTRY")){
				//t.set(entry[0]+":"+entry[1]);
				//v.set(entry[2]);
				t.set(entry[0]);
				v.set(entry[1]+":"+entry[2]);
				context.write(t, v);
			}
			
			else if(entry[0].equalsIgnoreCase("IP")){
				t.set(entry[0]);
				v.set(entry[1]);
				context.write(t, v);
			}

	}
	
}


