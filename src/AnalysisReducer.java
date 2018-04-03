/*
 	* Class name: AnalysisReducer (Reducer 2)
 	* 
 	* Done by: Daniel, Phoebe, YanHsia 
 	* 
 	* Description:
 	* This is the reducer responsible for generating the final output for
 	* tasks 1, 2, 3, 4 and 7. Along with several task 9 extra features such as sentiment comparison built in.
 	* It reads in the data from the /temp folder and outputs the the /output folder
 	* 
*/
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.json.*;

import java.io.BufferedReader;
import java.io.FileReader;
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

public class AnalysisReducer extends Reducer<Text, Text, Text, Text> {
	Text v = new Text();
	Text k = new Text();
	
	//For Task 1
	private HashMap airlineMap = new HashMap<String, HashMap>();
	private HashMap<String,Integer> reasonsMap = new HashMap<String,Integer>();
	
	//For Task 2 and 3
	private HashMap countryMap = new HashMap<String, HashMap>();
	private HashMap<String,Integer> totalReasonsByCountryMap = new HashMap<String,Integer>();
	
	//For Task 4 
	private HashMap positiveByAirlineMap = new HashMap<String, Integer>();
	private HashMap neutralByAirlineMap = new HashMap<String, Integer>();
	
	//For Task 7
	private HashMap IPAddressMap = new HashMap<String, Integer>();
	
	//Extras
	private HashMap positiveByCountry = new HashMap<String, Integer>();
	private HashMap neutralByCountry = new HashMap<String, Integer>();
	
	private Hashtable countryCodes = new Hashtable<String, String>();
	
	private boolean isTask4 = false;
	
	protected void setup() throws IOException, InterruptedException {
		BufferedReader br =new BufferedReader(new FileReader("ISO-3166-alpha3.tsv"));
		String line = null;
		while(true){
			line=br.readLine();
			if(line != null){
				String parts[]= line.split("\t");
				countryCodes.put(parts[0], parts[1]);
			}
			else{
				break;
			}
		}
		br.close();
	}
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		
		String[] entry = key.toString().split(":");
		
		if(entry[0].equalsIgnoreCase("NEGATIVEBYAIRLINE")){
			for(Text t: values){
				String[] parts = t.toString().split(":");
				
				//This sends positive and neutral values to the reducer for task 1
				if(parts.length==3){//AIRLINE-NEUTRAL::airline\tcount
					String split[] = parts[2].toString().split("\t");
					int total = Integer.valueOf(split[1]);
					
					if(parts[0].equalsIgnoreCase("AIRLINE-POSITIVE")){
						positiveByAirlineMap.put(split[0], total);
					}
					else if(parts[0].equalsIgnoreCase("AIRLINE-NEUTRAL")){
						neutralByAirlineMap.put(split[0], total);
					}
				}
				else{
					String airline = parts[0];
					
					String[] subValues = parts[1].split("\t");
					String issue = subValues[0];
					int total = Integer.valueOf(subValues[1]);
					
					//Add to hashmap of discrete reasons
					if(!reasonsMap.containsKey(issue)){
						reasonsMap.put(issue,1);
					}
					
					//Add to individual airline hashmaps
					if(airlineMap.containsKey(airline)){
						HashMap<String,Integer> negativeReasonMap = (HashMap) airlineMap.get(airline);
						negativeReasonMap.put(issue, total);
						airlineMap.put(airline,negativeReasonMap);
					}
					else{
						//Create new key
						HashMap<String,Integer> negativeReasonMap = new HashMap<String, Integer>();
						negativeReasonMap.put(issue, total);
						airlineMap.put(airline,negativeReasonMap);
					}
				}
				
				
				
				
			}
		}
		//KEY: NEGATIVEBYCOUNTRY:[Code]		VALUE:[Reason] [count]
		else if (entry[0].equalsIgnoreCase("NEGATIVEBYCOUNTRY")){ 
			for(Text t: values){
				String[] parts = t.toString().split(":");
				
				if(parts.length==3){//AIRLINE-NEUTRAL::airline\tcount
					String split[] = parts[2].toString().split("\t");
					int total = Integer.valueOf(split[1]);
					
					if(parts[0].equalsIgnoreCase("COUNTRY-POSITIVE")){
						positiveByCountry.put(split[0], total);
					}
					else if(parts[0].equalsIgnoreCase("COUNTRY-NEUTRAL")){
						neutralByCountry.put(split[0], total);
					}
				}
				else{
					String country = parts[0];
					
					String[] subValues = parts[1].toString().split("\t");
					String issue = subValues[0];
					int total = Integer.valueOf(subValues[1]);
						
					//Add to individual country hashmaps
					if(countryMap.containsKey(country)){
						HashMap<String,Integer> reasonsSingleCountryMap = (HashMap) countryMap.get(country);
						reasonsSingleCountryMap.put(issue, total);
						countryMap.put(country,reasonsSingleCountryMap);
						
						int tempCount = totalReasonsByCountryMap.get(country);
						totalReasonsByCountryMap.put(country, total+tempCount);
					}
					else{
						//Create new key
						HashMap<String,Integer> reasonsSingleCountryMap = new HashMap<String, Integer>();
						reasonsSingleCountryMap.put(issue, total);
						countryMap.put(country,reasonsSingleCountryMap);
						
						totalReasonsByCountryMap.put(country, total);
					}
				}
				
				
			}
		}
		else if (entry[0].equalsIgnoreCase("SENTIMENT")){
			isTask4 = true;
			for(Text t: values){
				String[] parts = t.toString().split(":");
				String split[] = parts[1].toString().split("\t");
				int total = Integer.valueOf(split[1]);
				
				if(parts[0].equalsIgnoreCase("AIRLINE-POSITIVE")){
					positiveByAirlineMap.put(split[0], total);
				}
				else if(parts[0].equalsIgnoreCase("AIRLINE-NEUTRAL")){
					neutralByAirlineMap.put(split[0], total);
				}
				else if(parts[0].equalsIgnoreCase("COUNTRY-POSITIVE")){
					positiveByCountry.put(split[0], total);
				}
				else if(parts[0].equalsIgnoreCase("COUNTRY-NEUTRAL")){
					neutralByCountry.put(split[0], total);
				}
			}
		}
		
		else if (entry[0].equalsIgnoreCase("IP")){
			//String airline = entry[1];
			for(Text t: values){
				String split[] = t.toString().split("\t");
				int total = Integer.valueOf(split[1]);
				IPAddressMap.put(split[0], total);
			}
		}
		
	}
	
	@Override
    public void cleanup(Context context) throws IOException, InterruptedException {
		setup();//loads the ISO country codes
		//check if there is a any values in the relevant hashmap before calling the display function.
		
		//Task 1 
		if(!airlineMap.isEmpty()){
			displayAirlineData(context);
			//task1JSON(context);
		}
		//Task 2 & 3
		if(!totalReasonsByCountryMap.isEmpty()){
			displayCountryData(context);
			//task23JSON(context);
		}
		//Task 4
		if(isTask4){
			top3Airlines(context);
			//task4JSON(context);
		}
		//Task 7
		if(!IPAddressMap.isEmpty()){
			ipAddresses(context);
			//task7JSON(context);
		}
		
		
		
		
    
    }
	
	public void displayAirlineData(Context context) throws IOException, InterruptedException{
		//==================== Output Top 5 Complains by Airline ====================
		k.set("=========================== TASK 1 ===========================\nNumber of Negative Reasons");
		v.set(String.valueOf(reasonsMap.size()));
		context.write(k, v);
		Iterator it = airlineMap.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        String airline = pair.getKey().toString();
	        
	        
	        //positiveByAirlineMap.get(key).toString()
	        
	        HashMap<String,Integer> currentMap = (HashMap)pair.getValue();
	        LinkedHashMap<String,Integer> sortedMap = sortHashMapByValues(currentMap);
	        int counter = 0;
	        int subTotal = 0;
		    
		    final Set<String> keys = sortedMap.keySet();
		    List<String> list = new ArrayList<String>();
		    
		    //Get just the bottom 5 aka the largest values in the sortedHashmap
		    for (final String key : keys) {
		    	if(counter>keys.size()-6){
		    		list.add(key);
			    	subTotal+=Integer.valueOf((int)sortedMap.get(key));
		    	}
		    	counter++;
		    }
		    Collections.reverse(list); //reverse to show top 5 in decreasing order
		    
		    k.set("\n====="+airline+"=====");
		    v.set("");
		    context.write(k, v);
		    
		    k.set("Sentiment\nPOSITIVE");
		    if(positiveByAirlineMap.containsKey(airline)){
		    	v.set(positiveByAirlineMap.get(airline).toString()); 
		    }
		    else{
		    	v.set("0"); 
		    }
	    	context.write(k, v);
		    
		    k.set("NEUTRAL");
		    if(neutralByAirlineMap.containsKey(airline)){
		    	v.set(neutralByAirlineMap.get(airline).toString());
		    }
		    else{
		    	v.set("0"); 
		    }
		    context.write(k, v);
		    
		    k.set("NEGATIVE");
		    v.set(String.valueOf(subTotal));
		    context.write(k, v);
		    
		    k.set("\nREASONS");
		    v.set("");
		    context.write(k, v);
		    for(final String key : list){
		    	k.set(key);//show problem eg CSProblem with counter
		    	v.set(currentMap.get(key).toString()); //show number of problems found
		    	context.write(k, v);
		    }
	        it.remove(); // avoids a ConcurrentModificationException
	    }
	}
	
	public void task1JSON(Context context) throws IOException, InterruptedException{
		JSONObject task1JSON = new JSONObject();
		JSONArray aArr = new JSONArray();
		Iterator it = airlineMap.entrySet().iterator();
		try{
			while (it.hasNext()) {
				JSONObject output = new JSONObject();
		        Map.Entry pair = (Map.Entry)it.next();
		        String airline = pair.getKey().toString();
		        
		        HashMap<String,Integer> currentMap = (HashMap)pair.getValue();
		        LinkedHashMap<String,Integer> sortedMap = sortHashMapByValues(currentMap);
		        int counter = 0;
		        int subTotal = 0;
			    
			    final Set<String> keys = sortedMap.keySet();
			    List<String> list = new ArrayList<String>();
			    
			    //Get just the bottom 5 aka the largest values in the sortedHashmap
			    for (final String key : keys) {
			    	if(counter>keys.size()-6){
			    		list.add(key);
				    	subTotal+=Integer.valueOf((int)sortedMap.get(key));
			    	}
			    	counter++;
			    }
			    Collections.reverse(list); //reverse to show top 5 in decreasing order
			    
			    JSONObject pos = new JSONObject();
			    JSONObject neu = new JSONObject();
			    JSONObject neg = new JSONObject();
			    if(positiveByAirlineMap.containsKey(airline)){
			    	pos.put("POSITVE", positiveByAirlineMap.get(airline));
			    }
			    else{
			    	pos.put("POSITVE", 0);
			    }
			    if(neutralByAirlineMap.containsKey(airline)){
			    	neu.put("NEUTRAL", neutralByAirlineMap.get(airline));
			    }
			    else{
			    	neu.put("NEUTRAL", 0);
			    }
			    neg.put("NEGATIVE", subTotal);
			    
			    JSONArray rArr = new JSONArray();
			    for(final String key : list){
			    	JSONObject reasonsJSON = new JSONObject();
			    	reasonsJSON.put(key, currentMap.get(key));
			    	rArr.put(reasonsJSON);
			    }
			    
			    output.put("name", airline);
			    JSONArray sArr = new JSONArray();
			    sArr.put(pos);
			    sArr.put(neu);
			    sArr.put(neg);
			    output.put("sentiment", sArr);
			    output.put("totalReasons", subTotal);
			    
			    
			    output.put("reasons", rArr);
			    aArr.put(output);
			    
		        it.remove(); // avoids a ConcurrentModificationException
		    }
			task1JSON.put("airlines", aArr);
			k.set(task1JSON.toString());
		    v.set("");
		    context.write(k, v);
		}
		catch(JSONException ex){}
	    
	}
	
	public void displayCountryData(Context context) throws IOException, InterruptedException{
		k.set("\n=========================== TASK 2 & 3 ===========================\nNumber of Countries");
    	v.set(String.valueOf(countryMap.size()));
    	context.write(k, v);
    	
    	LinkedHashMap<String,Integer> sortedTotalsMap = sortHashMapByValues(totalReasonsByCountryMap);
    	final Set<String> totalsKeys = sortedTotalsMap.keySet();
    	List<String> totalsList = new ArrayList<String>();
    	for (final String key : totalsKeys) {
	    	totalsList.add(key);
	    }
	    Collections.reverse(totalsList);
    	String topCountry = totalsList.get(0);
    	k.set("Country with most complains: "+countryCodes.get(topCountry));
    	v.set(String.valueOf(totalReasonsByCountryMap.get(topCountry)));
    	context.write(k, v);
    	
    	for (final String countrykey : totalsList) {
	        
	        HashMap<String,Integer> currentMap = (HashMap<String,Integer>)countryMap.get(countrykey);
	        
	        LinkedHashMap<String,Integer> sortedMap = sortHashMapByValues(currentMap);
	        int counter = 0;
	        int subTotal = 0;
		    
		    final Set<String> keys = sortedMap.keySet();
		    List<String> list = new ArrayList<String>();
		    
		    //Get just the bottom 5 aka the largest values in the sortedHashmap
		    for (final String key : keys) {
		    	list.add(key);
		    	subTotal+=Integer.valueOf((int)sortedMap.get(key));
		    	counter++;
		    }
		    Collections.reverse(list); //reverse to show top 5 in decreasing order
		    
		   
		    
		    k.set("====="+countryCodes.get(countrykey)+"=====");
		    v.set("");
		    context.write(k, v);
		    
		    k.set("Positive");
		    if(positiveByCountry.containsKey(countrykey)){
		    	int num = (int) positiveByCountry.get(countrykey);
		    	v.set(String.valueOf(num));
		    }
		    else{
		    	v.set("0");
		    }
		    context.write(k, v);
		    
		    k.set("Neutral");
		    if(neutralByCountry.containsKey(countrykey)){
		    	int num = (int) neutralByCountry.get(countrykey);
		    	v.set(String.valueOf(num));
		    }
		    else{
		    	v.set("0");
		    }
		    context.write(k, v);
		    
		    k.set("Negative");
		    v.set(String.valueOf(subTotal));
		    context.write(k, v);
		    
		    String reason="";
		    for(final String key : list){
		    	reason=key;
		    	if(reason.equalsIgnoreCase("CSProblem")  || reason.equalsIgnoreCase("badflight")){
		    		k.set("-"+reason);//show problem eg CSProblem with counter
			    	v.set(currentMap.get(key).toString()); //show number of problems found
			    	context.write(k, v);
		    	}
		    	
		    }
	    }
	}
	
	public void task23JSON(Context context) throws IOException, InterruptedException{
		JSONObject task23JSON = new JSONObject();
		JSONArray aArr = new JSONArray();
		JSONObject output = new JSONObject();
		JSONArray lcArr = new JSONArray();
		
		try{
	    	
	    	LinkedHashMap<String,Integer> sortedTotalsMap = sortHashMapByValues(totalReasonsByCountryMap);
	    	final Set<String> totalsKeys = sortedTotalsMap.keySet();
	    	List<String> totalsList = new ArrayList<String>();
	    	for (final String key : totalsKeys) {
		    	totalsList.add(key);
		    }
		    Collections.reverse(totalsList);
	    	String topCountry = totalsList.get(0);
	    	
	    	output.put("numberOfCountries", countryMap.size());
		    output.put("mostComplaintsCountry", countryCodes.get(topCountry));
		    output.put("mostComplaintsNumber", totalReasonsByCountryMap.get(topCountry));
	    	for (final String countrykey : totalsList) {
	    		JSONObject countryJSON = new JSONObject();
	    		
		        
		        HashMap<String,Integer> currentMap = (HashMap<String,Integer>)countryMap.get(countrykey);
		        
		        LinkedHashMap<String,Integer> sortedMap = sortHashMapByValues(currentMap);
		        int counter = 0;
		        int subTotal = 0;
			    
			    final Set<String> keys = sortedMap.keySet();
			    List<String> list = new ArrayList<String>();
			    
			    //Get just the bottom 5 aka the largest values in the sortedHashmap
			    for (final String key : keys) {
			    	list.add(key);
			    	subTotal+=Integer.valueOf((int)sortedMap.get(key));
			    	counter++;
			    }
			    Collections.reverse(list); //reverse to show top 5 in decreasing order
			    
			   countryJSON.put("name", countryCodes.get(countrykey));
			    
			    if(positiveByCountry.containsKey(countrykey)){
			    	int num = (int) positiveByCountry.get(countrykey);
			    	countryJSON.put("POSITIVE", num);
			    }
			    else{
			    	countryJSON.put("POSITIVE", 0);
			    }
			   
			    if(neutralByCountry.containsKey(countrykey)){
			    	int num = (int) neutralByCountry.get(countrykey);
			    	countryJSON.put("NEUTRAL", num);
			    }
			    else{
			    	countryJSON.put("NEUTRAL", 0);
			    }
			    countryJSON.put("NEGATIVE", subTotal);
			    
			    
			    JSONArray nrArr = new JSONArray();
			    
			    String reason="";
			    for(final String key : list){
			    	
			    	reason=key;
			    	if(reason.equalsIgnoreCase("CSProblem")  || reason.equalsIgnoreCase("badflight")){
				    	JSONObject nrObj = new JSONObject();
				    	nrObj.put(reason,currentMap.get(key));
				    	nrArr.put(nrObj);
			    	}
			    	
			    }
			    
			    countryJSON.put("reasons",nrArr);
			    lcArr.put(countryJSON);
		    }
	    	
	    	
	    	
	    	
		    output.put("listOfCountries", lcArr);
		    aArr.put(output);
		    task23JSON.put("complaints", aArr);
			k.set(task23JSON.toString());
		    v.set("");
		    context.write(k, v);
		}
		catch(JSONException ex){}
		
	}

	public void top3Airlines(Context context) throws IOException, InterruptedException{
		k.set("\n=========================== TASK 4 ===========================\nTop 3 Airlines with Positive Tweets");
    	v.set(String.valueOf(positiveByAirlineMap.size()));
    	context.write(k, v);

    	LinkedHashMap<String,Integer> sortedMap = sortHashMapByValues(positiveByAirlineMap);
    	
	    final Set<String> keys = sortedMap.keySet();
	    List<String> list = new ArrayList<String>();
	    
	    for (final String key : keys) {
	    	list.add(key);
	    }
	    Collections.reverse(list);
	    int counter=0;
	    for(final String key : list){
	    	if(counter<3){
	    		k.set(counter+1+") "+key);//show problem eg CSProblem with counter
		    	v.set(positiveByAirlineMap.get(key).toString()); //show number of problems found
		    	context.write(k, v);
	    	}
	    	counter++;
	    }
		
	}
	
	public void task4JSON(Context context) throws IOException, InterruptedException{
		JSONObject task4JSON = new JSONObject();
		JSONArray aArr = new JSONArray();
		try{
	    	LinkedHashMap<String,Integer> sortedMap = sortHashMapByValues(positiveByAirlineMap);
	    	
		    final Set<String> keys = sortedMap.keySet();
		    List<String> list = new ArrayList<String>();
		    
		    for (final String key : keys) {
		    	list.add(key);
		    }
		    Collections.reverse(list);
		    int counter=0;
		    for(final String key : list){
		    	if(counter<3){
			    	JSONObject airlineItem = new JSONObject();
			    	airlineItem.put(key,(int)positiveByAirlineMap.get(key));
			    	aArr.put(airlineItem);
		    	}
		    	counter++;
		    }
			
		    task4JSON.put("top3Airlines", aArr);
			k.set(task4JSON.toString());
		    v.set("");
		    context.write(k, v);
		}
		catch(JSONException ex){}
	}
	
	public void ipAddresses(Context context) throws IOException, InterruptedException{
		k.set("\n=========================== TASK 7 ===========================\nUnique IPs in Twitter Dataset");
    	v.set(String.valueOf(IPAddressMap.size()));
    	context.write(k, v);

    	LinkedHashMap<String,Integer> sortedMap = sortHashMapByValues(IPAddressMap);
    	
	    final Set<String> keys = sortedMap.keySet();
	    List<String> list = new ArrayList<String>();
	    
	    for (final String key : keys) {
	    	list.add(key);
	    }
	    Collections.reverse(list);
	    for(final String key : list){
	    	k.set(key);//show ip 
		    v.set(IPAddressMap.get(key).toString()+" tweets"); //show number of tweets found
		    context.write(k, v);
	    	
	    }
	}
	public void task7JSON(Context context) throws IOException, InterruptedException{
		JSONObject task7JSON = new JSONObject();
		JSONArray aArr = new JSONArray();
		JSONObject output = new JSONObject();
		JSONArray ipArr = new JSONArray();
		try{
	    	LinkedHashMap<String,Integer> sortedMap = sortHashMapByValues(IPAddressMap);
	    	
		    final Set<String> keys = sortedMap.keySet();
		    List<String> list = new ArrayList<String>();
		    
		    for (final String key : keys) {
		    	list.add(key);
		    }
		    Collections.reverse(list);
		    for(final String key : list){
			    JSONObject ipItem = new JSONObject();
			    ipItem.put(key,IPAddressMap.get(key));
			    ipArr.put(ipItem);
		    }
		    
	    	
	    	output.put("uniqueIPs", IPAddressMap.size());
	    	output.put("listOfIPs",ipArr);
	    	aArr.put(output);
	    	task7JSON.put("IPs", aArr);
			k.set(task7JSON.toString());
		    v.set("");
		    context.write(k, v);
		}
		catch(JSONException ex){}
	}
	
	
	public static LinkedHashMap<String, Integer> sortHashMapByValues(
	        HashMap<String, Integer> passedMap) {
	    List<String> mapKeys = new ArrayList<>(passedMap.keySet());
	    List<Integer> mapValues = new ArrayList<>(passedMap.values());
	    Collections.sort(mapValues);
	    Collections.sort(mapKeys);

	    LinkedHashMap<String, Integer> sortedMap =
	        new LinkedHashMap<>();
	   
	    Iterator<Integer> valueIt = mapValues.iterator();
	    while (valueIt.hasNext()) {
	        Integer val = valueIt.next();
	        Iterator<String> keyIt = mapKeys.iterator();

	        while (keyIt.hasNext()) {
	        		String key = keyIt.next();
	            Integer comp1 = passedMap.get(key);
	            Integer comp2 = val;

	            if (comp1.equals(comp2)) {
	                keyIt.remove();
	                sortedMap.put(key, val);
	                break;
	            }
	        }
	    }
	    return sortedMap;
	}
}