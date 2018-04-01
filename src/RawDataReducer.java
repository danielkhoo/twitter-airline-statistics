/*
 	* Class name: TwitterAnalytics (Driver Class)
 	* 
 	* Done by: Daniel
 	* 
 	* Description:
 	* This is the reducer for aggregating all the values that need counting, 
 	* it receives the values and sums them for all our tasks
 	* 
 	* 
*/
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

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

public class RawDataReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	IntWritable totalIW = new IntWritable();
	Text k = new Text();
	Text v = new Text();
	private Hashtable kvTable = new Hashtable<String, Integer>();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
		int total = 0;
		for (IntWritable value : values) {
			total += value.get();
		}
        // puts the number of occurrences of this word into the map.
        //countMap.put(key.toString(), total);
        totalIW.set(total);
		context.write(key, totalIW);
	}
	
	@Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        
    }
}