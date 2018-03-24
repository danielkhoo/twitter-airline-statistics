import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
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
	
	//For Task 7
	private HashMap IPAddressMap = new HashMap<String, Integer>();
	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException {
	
		String[] entry = key.toString().split(":");
		
		if(entry[0].equalsIgnoreCase("NEGATIVEBYAIRLINE")){
			String airline = entry[1];
			
			for(Text t: values){
				String[] subValues = t.toString().split("\t");
				String issue = subValues[0];
				int total = Integer.valueOf(subValues[1]);
				
				//Add to hashmap of discrete reasons
				if(!reasonsMap.containsKey(airline)){
					reasonsMap.put(airline,1);
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
		//KEY: NEGATIVEBYCOUNTRY:[Code]		VALUE:[Reason] [count]
		else if (entry[0].equalsIgnoreCase("NEGATIVEBYCOUNTRY")){ 	
			//Clarify with ZK regarding task 3 whether it need to be by airline/country/reason or just country reason
			String country = entry[1];
			for(Text t: values){
				String[] subValues = t.toString().split("\t");
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
		else if (entry[0].equalsIgnoreCase("AIRLINE-POS")){
			//String airline = entry[1];
			for(Text t: values){
				String split[] = t.toString().split("\t");
				int total = Integer.valueOf(split[1]);
				positiveByAirlineMap.put(split[0], total);
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
		
		top5Reasons(context);
		
		negativeReasonsByCountry(context);
		
		top3Airlines(context);
		
		ipAddresses(context);
    
    }
	
	public void top5Reasons(Context context) throws IOException, InterruptedException{
		//==================== Output Top 5 Complains by Airline ====================
		k.set("=========================== TASK 1 ===========================\nNumber of Negative Reasons");
    	v.set(String.valueOf(reasonsMap.size()));
    	context.write(k, v);
    	
				Iterator it = airlineMap.entrySet().iterator();
			    while (it.hasNext()) {
			        Map.Entry pair = (Map.Entry)it.next();
			        //System.out.println(pair.getKey() + " = " + pair.getValue());
			        
			        HashMap<String,Integer> currentMap = (HashMap)pair.getValue();
			        LinkedHashMap<String,Integer> sortedMap = sortHashMapByValues(currentMap);
			        int counter = 0;
			        int subTotal = 0;
				    
				    final Set<String> keys = sortedMap.keySet();
				    List<String> list = new ArrayList<String>();
				    
				    //Get just the bottom 5 aka the largest values in the sortedHashmap
				    for (final String key : keys) {
				    	list.add(key);
				    	subTotal+=Integer.valueOf((int)sortedMap.get(key));
				    }
				    Collections.reverse(list); //reverse to show top 5 in decreasing order

				    k.set("====="+pair.getKey().toString()+"=====");
				    v.set(String.valueOf(subTotal));
				    context.write(k, v);
				    
				    for(final String key : list){
				    	k.set(key);//show problem eg CSProblem with counter
				    	v.set(currentMap.get(key).toString()); //show number of problems found
				    	context.write(k, v);
				    }
			        it.remove(); // avoids a ConcurrentModificationException
			    }
	}
	
	public void negativeReasonsByCountry(Context context) throws IOException, InterruptedException{
		//==================== Output Top 5 Complains by Airline ====================
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
    	k.set("Country with most complains: "+topCountry);
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
		    
		    k.set("====="+countrykey+"=====");
		    v.set(String.valueOf(subTotal));
		    context.write(k, v);
		    
		    String reason="";
		    for(final String key : list){
		    	reason=key;
		    	if(reason.equalsIgnoreCase("CSProblem")  || reason.equalsIgnoreCase("badflight")){
		    		k.set(reason);//show problem eg CSProblem with counter
			    	v.set(currentMap.get(key).toString()); //show number of problems found
			    	context.write(k, v);
		    	}
		    	
		    }
    		
	    }
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