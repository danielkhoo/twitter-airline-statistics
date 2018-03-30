import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

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
	private HashMap<String, ArrayList<String>> tweetByKeyword = new HashMap<String, ArrayList<String>>();
	int positiveMatch = 0;
	int positiveMismatch = 0;
	int negativeMatch = 0;
	int negativeMismatch = 0;
	
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
				
				//Split tweet into array of string
				String[] parts = t.toString().split(":"); //tweet has threes parts searchTerm:content:score
				String searchTerm=parts[0];
				String content = parts[1];
				String sentiment = parts[2];
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
		    	}
			}
		}
		else if(entry[0].equalsIgnoreCase("SENTIWORD")){
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
		
		//Phoebe's function for displaying the data -daniel
		trustpointCalculations(context);
		//task6(context);	
		//task8(context);
		
		//airlineSentimentScores(context);
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
	    	/*
	    	Collections.sort(currentlist);
	    	Double median;
	    	if(currentlist.size()%2==0){
	    		median = (currentlist.get(currentlist.size()/2)+currentlist.get((currentlist.size()/2)-1))/2;
	    	}
	    	else{
	    		median = currentlist.get(currentlist.size()/2);
	    	}
	    	*/
	    	
	    	k.set("\n"+key+"\tAverage: "+formatter.format(average));
	    	v.set("");
	    	context.write(k, v);
	    }
	}

	
	public void task6(Context context) throws IOException, InterruptedException{
		k.set("\n=========================== TASK 6 ===========================\n");
    	v.set("");
    	context.write(k, v);
    	
    	if(tweetByKeyword.size()>0){
    		final Set<String> keys = tweetByKeyword.keySet();
        	for (final String key : keys) {
    			ArrayList<String> worklist = tweetByKeyword.get(key);
    			k.set("keyword: \""+key+"\"");
    			v.set(worklist.size()+" occurences");
    			context.write(k, v);
    			
    			for(String word: worklist){
    				k.set("");
    				v.set(word);
    				context.write(k, v);
    			}
    	    }
    	}
	}
	
	public void task8(Context context) throws IOException, InterruptedException{
		k.set("\n=========================== TASK 8 ===========================");
		String output = "";
		if((positiveMatch+positiveMismatch)>0){
			output += "\nPositive Accuracy: "+String.valueOf(positiveMatch*100/(positiveMatch+positiveMismatch))+"%\nPositive Matches: "+String.valueOf(positiveMatch)+"\nPositve Mismatches: "+String.valueOf(positiveMismatch);
		}
		if((negativeMatch+negativeMismatch)>0){
			output += "\nNegative Accuracy: "+String.valueOf(negativeMatch*100/(negativeMatch+negativeMismatch))+"%\nNegative Matches: "+String.valueOf(negativeMatch)+"\nNegative Mismatches: "+String.valueOf(negativeMismatch);
		}
		v.set(output);
		context.write(k, v);
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
	    	
	    	
	    	Collections.sort(trustpoints);
	    	Double median;
	    	if(trustpoints.size()%2==0){
	    		median = (trustpoints.get(trustpoints.size()/2)+trustpoints.get((trustpoints.size()/2)-1))/2;
	    	}
	    	else{
	    		median = trustpoints.get(trustpoints.size()/2);
	    	}
	    	
	    	
	    	
	    	
	    	//Double median = 0.0;//placeholder
	    	//=============================================================================
	    	
	    	
	    	//This code is to output the median 
	    	//output is shown in tweets folder
			k.set(key);
		    v.set(median.toString()); 
		    context.write(k, v);
	    }
		
	}	
	

}