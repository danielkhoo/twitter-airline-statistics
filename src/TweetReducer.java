import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TweetReducer extends Reducer<Text, Text, Text, Text> {
	IntWritable totalIW = new IntWritable();
	Text k = new Text();
	Text v = new Text();
	private HashMap<String, ArrayList<Double>> airlineMap = new HashMap<String, ArrayList<Double>>();
	
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		
		String[] entry = key.toString().split(":");
				
		if(entry[0].equalsIgnoreCase("TRUST")){
			String airline = entry[1];
			ArrayList<Double> trustpoints = new ArrayList<Double>();
			
			for(Text t: values){
				if(t.toString().matches("[0-9.]*")){
					trustpoints.add(Double.parseDouble(t.toString()));
				}
			}
			if(trustpoints.size()>0){
				airlineMap.put(airline,trustpoints);
			}
		}
		
		else if(entry[0].equalsIgnoreCase("TWEET")){
			for(Text t: values){
				
		    	//=============================================================================
		    	
		    	//				JOEY PLEASE ADD YOUR CODE HERE 
		    	//				t.toString() is basically the text of the tweet 
		    	//				Try to do the bulk of the logic here as it is distributed and faster
				//				Or add functions and call them from here
				//				The logic for displaying and formatting can be done in the cleanup function below
				//				Good luck! 
				//				-daniel
		    	
		    	
		    	
		    	
		    	
		    	Double median = 0.0;//placeholder
		    	//=============================================================================
				
				k.set("");
				v.set(t.toString());
				context.write(k, v);//this just prints out the text whole
			}
		}
	}
	
	@Override
    public void cleanup(Context context) throws IOException, InterruptedException {
		
		//Phoebe's function for displaying the data -daniel
		trustpointCalculations(context);
		
		
    }
	
	public void trustpointCalculations(Context context) throws IOException, InterruptedException{
		k.set("\n=========================== TASK 5 ===========================\nNumber of Airlines");
    	v.set(String.valueOf(airlineMap.size()));
    	context.write(k, v);
		
		final Set<String> keys = airlineMap.keySet();
	    
	    for (final String key : keys) {
	    	//Get the array of trustpoint values of a single airline
	    	ArrayList<Double> trustpoints = (ArrayList) airlineMap.get(key);
	    	
	    	
	    	//=============================================================================
	    	
	    	//				PHOEBE PLEASE ADD YOUR CODE HERE 
	    	//				Calculate and display the median of the trustpoint array
	    	//				Btw comment out line 66 if you want to hide Joey's section
	    	//				Happy coding!
	    	//				- daniel
	    	
	    	
	    	
	    	
	    	
	    	Double median = 0.0;//placeholder
	    	//=============================================================================
	    	
	    	
	    	//This code is to output the median 
	    	//output is shown in tweets folder
			k.set(key);
		    v.set(median.toString()); 
		    context.write(k, v);
	    }
		
	}	
}