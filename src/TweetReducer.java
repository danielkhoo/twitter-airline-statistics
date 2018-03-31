import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
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
	//private HashMap<String, ArrayList<String>> tweetByKeyword = new HashMap<String, ArrayList<String>>();
	private HashMap<String, Integer> tweetByKeyword = new HashMap<String, Integer>();
	int positiveMatch = 0;
	int positiveMismatch = 0;
	int negativeMatch = 0;
	int negativeMismatch = 0;
	boolean isTask8=false;
	
	private HashMap<String, ArrayList<Double>> airlineSentimentMap = new HashMap<String, ArrayList<Double>>();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		
		String[] entry = key.toString().split(":");
				
		if(entry[0].equalsIgnoreCase("TRUST")){
			
			ArrayList<Double> trustpoints = new ArrayList<Double>();
			for(Text t: values){
				
				String[] parts = t.toString().split(":"); 
				String airline = parts[0];
				
				
				if(parts[1].matches("[0-9.]*")){
				
					if(airlineMap.containsKey(airline)){
			    		ArrayList<Double> trustscore = (ArrayList<Double>) airlineMap.get(airline);
			    		trustscore.add(Double.parseDouble(parts[1].toString()));
			    		airlineMap.put(airline, trustscore);
			    	}
			    	else{
			    		ArrayList<Double> trustscore = new ArrayList<Double>();
			    		trustscore.add(Double.parseDouble(parts[1].toString()));
			    		airlineMap.put(airline, trustscore);
			    	}
				}
				
				
			}
			
		}
		
		else if(entry[0].equalsIgnoreCase("TWEET")){
			for(Text t: values){
				String searchTerm=t.toString();
				if(tweetByKeyword.containsKey(searchTerm)){
		    		Integer count = tweetByKeyword.get(searchTerm);
		    		tweetByKeyword.put(searchTerm, count+1);
		    	}
		    	else{
		    		tweetByKeyword.put(searchTerm, 1);
		    	}
				/*
				//Split tweet into array of string
				String[] parts = t.toString().split(":"); //tweet has threes parts searchTerm:content:score
				String searchTerm=parts[0];
				String content = parts[1];
				String sentiment = parts[1];
				//String[] wordlist = content.toString().split(" ");
				
				if(tweetByKeyword.containsKey(searchTerm)){
		    		ArrayList<String> worklist = (ArrayList<String>) tweetByKeyword.get(searchTerm);
		    		worklist.add("Sentiment: "+sentiment+"\n\tTweet: "+content+"\n");
		    		tweetByKeyword.put(searchTerm, worklist);
		    	}
		    	else{
		    		ArrayList<String> worklist = new ArrayList<String>();
		    		worklist.add("Sentiment: "+sentiment+"\n\tTweet: "+content+"\n");
		    		tweetByKeyword.put(searchTerm, worklist);
		    	}*/
			}
		}
		else if(entry[0].equalsIgnoreCase("SENTIWORD")){
			isTask8 = true;
			for(Text t: values){
				//Split tweet into array of string
				String[] parts = t.toString().split(":"); //SENTIWORD has 4 parts match:statesenti:calcdsenti:pos/neg
				
				if(parts[0].equalsIgnoreCase("match")){
					if(parts[3].equalsIgnoreCase("positive")){
						positiveMatch += 1;
					}
					else{
						negativeMatch += 1;
					}
				}
				else{
					if(parts[3].equalsIgnoreCase("positive")){
						positiveMismatch += 1;
					}
					else{
						negativeMismatch += 1;
					}
				}
			}
		}
		
		else if(entry[0].equalsIgnoreCase("SENTIMENTBYAIRLINE")){ //airline:sentimentscore
			for(Text t: values){
				String[] parts = t.toString().split(":");
				String airline = parts[0];
				
				if(airlineSentimentMap.containsKey(airline)){
		    		ArrayList<Double> score = (ArrayList<Double>) airlineSentimentMap.get(airline);
		    		score.add(Double.parseDouble(parts[1].toString()));
		    		airlineSentimentMap.put(airline, score);
		    	}
		    	else{
		    		ArrayList<Double> score = new ArrayList<Double>();
		    		score.add(Double.parseDouble(parts[1].toString()));
		    		airlineSentimentMap.put(airline, score);
		    	}
			}
			
		}
	}
	
	@Override
    public void cleanup(Context context) throws IOException, InterruptedException {
		
		if(airlineMap.size()>0){
			//trustpointCalculations(context);
			task5JSON(context);
		}
		
		if(tweetByKeyword.size()>0){
			//task6(context);	
			task6JSON(context);
		}
		
		if(isTask8){
			//task8(context);
			task8JSON(context);
		}
		if(!airlineSentimentMap.isEmpty()){
			//airlineSentimentScores(context);
			extraJSON(context);
		}
		
    }
	

	public void trustpointCalculations(Context context) throws IOException, InterruptedException{
		k.set("\n=========================== TASK 5 ===========================\nNumber of Airlines");
    	v.set(String.valueOf(airlineMap.size()));
    	context.write(k, v);
		
		final Set<String> keys = airlineMap.keySet();
	    
	    for (final String key : keys) {
	    	//Get the array of trustpoint values of a single airline
	    	ArrayList<Double> trustpoints = (ArrayList) airlineMap.get(key);
	    	
	    	
	    	Collections.sort(trustpoints);
	    	Double median;
	    	if(trustpoints.size()%2==1){
	    		median = trustpoints.get(trustpoints.size()/2);
	    	}
	    	else{
	    		median = (trustpoints.get(trustpoints.size()/2)+trustpoints.get((trustpoints.size()/2)-1))/2;
	    	}
			k.set(key);
		    v.set(median.toString()); 
		    context.write(k, v);
	    }
	}	
	
	public void task6(Context context) throws IOException, InterruptedException{
		k.set("\n=========================== TASK 6 ===========================\nKeywords:");
    	v.set("");
    	context.write(k, v);
    	final Set<String> keys = tweetByKeyword.keySet();
    	for (final String key : keys) {
			//ArrayList<String> worklist = tweetByKeyword.get(key);
			k.set("\t\""+key+"\"");
			//v.set(worklist.size()+" occurences");
			v.set(tweetByKeyword.get(key)+" occurences");
			context.write(k, v);
			
			/*
			for(String word: worklist){
				k.set("");
				v.set(word);
				context.write(k, v);
			}*/
	    }
    	
	}
	
	public void task8(Context context) throws IOException, InterruptedException{
		k.set("\n=========================== TASK 8 ===========================");
		String output = "";
		if((positiveMatch+positiveMismatch)>0){
			output += "\nPositive Accuracy: "+String.valueOf(positiveMatch*100/(positiveMatch+positiveMismatch))+"%\nPositive Matches: "+String.valueOf(positiveMatch)+"\nPositve Mismatches: "+String.valueOf(positiveMismatch);
		}
		else{
			output += "\nPositive Accuracy: 0%\nPositive Matches: 0\nPositive Mismatches: 0";
		}
		if((negativeMatch+negativeMismatch)>0){
			output += "\nNegative Accuracy: "+String.valueOf(negativeMatch*100/(negativeMatch+negativeMismatch))+"%\nNegative Matches: "+String.valueOf(negativeMatch)+"\nNegative Mismatches: "+String.valueOf(negativeMismatch);
		}
		else{
			output += "\nNegative Accuracy: 0%\nNegative Matches: 0\nNegative Mismatches: 0";
		}
		v.set(output);
		context.write(k, v);
	}

	public void airlineSentimentScores(Context context) throws IOException, InterruptedException{
		k.set("\n=========================== EXTRA ===========================\n");
    	v.set("");
    	context.write(k, v);
		
		final Set<String> airlineKeys = airlineSentimentMap.keySet();
		DecimalFormat formatter = new DecimalFormat("#0.0000");
    	for (final String key : airlineKeys) {
	    	ArrayList<Double> currentlist = (ArrayList<Double>) airlineSentimentMap.get(key);
	    	Double sum=0.0;
	    	for(Double entry : currentlist){
	    		sum+=entry;
	    	}
	    	Double average = sum/currentlist.size();
	    	
	    	k.set("\n"+key+"\tAverage: "+formatter.format(average));
	    	v.set("");
	    	context.write(k, v);
	    }
	}

	public void task5JSON(Context context) throws IOException, InterruptedException{
		JSONObject task5JSON = new JSONObject();
		JSONArray aArr = new JSONArray();
		try{
			final Set<String> keys = airlineMap.keySet();
		    
		    for (final String key : keys) {
		    	//Get the array of trustpoint values of a single airline
		    	ArrayList<Double> trustpoints = (ArrayList) airlineMap.get(key);
		    	
		    	
		    	Collections.sort(trustpoints);
		    	Double median;
		    	if(trustpoints.size()%2==1){
		    		median = trustpoints.get(trustpoints.size()/2);
		    	}
		    	else{
		    		median = (trustpoints.get(trustpoints.size()/2)+trustpoints.get((trustpoints.size()/2)-1))/2;
		    	}
		    	JSONObject airlineItem = new JSONObject();
		    	airlineItem.put(key,(double)median);
		    	aArr.put(airlineItem);
		    }
		    
		    task5JSON.put("trustpoints", aArr);
			k.set(task5JSON.toString());
		    v.set("");
		    context.write(k, v);
		}
		catch(JSONException ex){}
	}
	
	public void task6JSON(Context context) throws IOException, InterruptedException{
		JSONObject task6JSON = new JSONObject();
		JSONArray aArr = new JSONArray();
		try{
			final Set<String> keys = tweetByKeyword.keySet();
	    	for (final String key : keys) {
				
				JSONObject keywordItem = new JSONObject();
		    	keywordItem.put(key,tweetByKeyword.get(key));
		    	aArr.put(keywordItem);
		    }
		    
		    task6JSON.put("keywords", aArr);
		    k.set(task6JSON.toString());
		    v.set("");
		    context.write(k, v);
		}
		catch(JSONException ex){}
	}
	
	public void task8JSON(Context context) throws IOException, InterruptedException{
		JSONObject task8JSON = new JSONObject();
		JSONObject temp = new JSONObject();
		try{
			if((positiveMatch+positiveMismatch)>0){
				temp.put("positiveAccuracy", positiveMatch*100/(positiveMatch+positiveMismatch));
				temp.put("positiveMatches", positiveMatch);
				temp.put("positiveMismatches", positiveMismatch);
			}
			else{
				temp.put("positiveAccuracy", 0);
				temp.put("positiveMatches", 0);
				temp.put("positiveMismatches", 0);
			}
			if((negativeMatch+negativeMismatch)>0){
				temp.put("negativeAccuracy",  negativeMatch*100/(negativeMatch+negativeMismatch));
				temp.put("negativeMatches", negativeMatch);
				temp.put("negattiveMismatches", negativeMismatch);
			}
			else{
				temp.put("negativeAccuracy",  0);
				temp.put("negativeMatches", 0);
				temp.put("negattiveMismatches", 0);
			}
		    
		    task8JSON.put("sentimentAccuracy", temp);
		    k.set(task8JSON.toString());
		    v.set("");
		    context.write(k, v);
		}
		catch(JSONException ex){}
	}
	
	public void extraJSON(Context context) throws IOException, InterruptedException{
		JSONObject extraJSON = new JSONObject();
		JSONArray aArr = new JSONArray();
		try{
			final Set<String> airlineKeys = airlineSentimentMap.keySet();
			DecimalFormat formatter = new DecimalFormat("#0.0000");
	    	for (final String key : airlineKeys) {
		    	ArrayList<Double> currentlist = (ArrayList<Double>) airlineSentimentMap.get(key);
		    	Double sum=0.0;
		    	for(Double entry : currentlist){
		    		sum+=entry;
		    	}
		    	Double average = sum/currentlist.size();
		    	JSONObject keywordItem = new JSONObject();
		    	keywordItem.put(key,formatter.format(average));
		    	aArr.put(keywordItem);
		    }
		    
		    extraJSON.put("averageSentiments", aArr);
		    k.set(extraJSON.toString());
		    v.set("");
		    context.write(k, v);
		}
		catch(JSONException ex){}
	}
	
}