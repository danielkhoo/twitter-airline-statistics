/*
 	* Class name: AnalysisMapper (Mapper Class 2)
 	* 
 	* Done by: Daniel, Phoebe, YanHsia
 	* 
 	* Description:
 	* This is the mapper for assembling the various subtotals to fulfill
 	* tasks 1, 2, 3, 4 and 7. Along with extra feature which sends sentiment by country / airline.
 	* 
 	* 
 	* 
*/
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
				
				t.set("NEGATIVEBYAIRLINE");//send sentiment data to reducer for extra
				v.set(entry[0]+"::"+entry[1]);
				context.write(t, v);
			}
			else if(entry[0].equalsIgnoreCase("AIRLINE-NEUTRAL")){
				t.set("SENTIMENT");
				v.set(entry[0]+":"+entry[1]);
				context.write(t, v);
				
				t.set("NEGATIVEBYAIRLINE");
				v.set(entry[0]+"::"+entry[1]);
				context.write(t, v);
			}
			else if(entry[0].equalsIgnoreCase("COUNTRY-POSITIVE")){
				t.set("SENTIMENT");
				v.set(entry[0]+":"+entry[1]);
				context.write(t, v);
				
				t.set("NEGATIVEBYCOUNTRY");
				v.set(entry[0]+"::"+entry[1]);
				context.write(t, v);
			}
			else if(entry[0].equalsIgnoreCase("COUNTRY-NEUTRAL")){
				t.set("SENTIMENT");
				v.set(entry[0]+":"+entry[1]);
				context.write(t, v);
				
				t.set("NEGATIVEBYCOUNTRY");
				v.set(entry[0]+"::"+entry[1]);
				context.write(t, v);
			}
			
			//Change Negative by airline as the sole flag so they all go to the same reducer
			else if(entry[0].equalsIgnoreCase("NEGATIVEBYAIRLINE")){
				t.set(entry[0]);
				v.set(entry[1]+":"+entry[2]);
				context.write(t, v);
			}
			else if(entry[0].equalsIgnoreCase("NEGATIVEBYCOUNTRY")){
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


