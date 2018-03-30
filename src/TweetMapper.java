import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class TweetMapper extends Mapper <LongWritable, Text, Text, Text> {
	Text t = new Text();
	Text v = new Text();
	
	private Map<String, Double> dictionary;
	public void SentiWordNetDemoCode(String pathToSWN) throws IOException {
		// This is our main dictionary representation
		dictionary = new HashMap<String, Double>();

		// From String to list of doubles.
		HashMap<String, HashMap<Integer, Double>> tempDictionary = new HashMap<String, HashMap<Integer, Double>>();

		BufferedReader csv = null;
		try {
			csv = new BufferedReader(new FileReader(pathToSWN));
			int lineNumber = 0;

			String line;
			while ((line = csv.readLine()) != null) {
				lineNumber++;

				// If it's a comment, skip this line.
				if (!line.trim().startsWith("#")) {
					// We use tab separation
					String[] data = line.split("\t");
					String wordTypeMarker = data[0];

					// Example line:
					// POS ID PosS NegS SynsetTerm#sensenumber Desc
					// a 00009618 0.5 0.25 spartan#4 austere#3 ascetical#2
					// ascetic#2 practicing great self-denial;...etc

					// Is it a valid line? Otherwise, through exception.
					if (data.length != 6) {
						throw new IllegalArgumentException(
								"Incorrect tabulation format in file, line: "
										+ lineNumber);
					}

					// Calculate synset score as score = PosS - NegS
					Double synsetScore = Double.parseDouble(data[2])
							- Double.parseDouble(data[3]);

					// Get all Synset terms
					String[] synTermsSplit = data[4].split(" ");

					// Go through all terms of current synset.
					for (String synTermSplit : synTermsSplit) {
						// Get synterm and synterm rank
						String[] synTermAndRank = synTermSplit.split("#");
						String synTerm = synTermAndRank[0];// + "#"+ wordTypeMarker; remove word type

						int synTermRank = Integer.parseInt(synTermAndRank[1]);
						// What we get here is a map of the type:
						// term -> {score of synset#1, score of synset#2...}

						// Add map to term if it doesn't have one
						if (!tempDictionary.containsKey(synTerm)) {
							tempDictionary.put(synTerm,
									new HashMap<Integer, Double>());
						}

						// Add synset link to synterm
						tempDictionary.get(synTerm).put(synTermRank,
								synsetScore);
					}
				}
			}

			// Go through all the terms.
			for (Map.Entry<String, HashMap<Integer, Double>> entry : tempDictionary
					.entrySet()) {
				String word = entry.getKey();
				Map<Integer, Double> synSetScoreMap = entry.getValue();

				// Calculate weighted average. Weigh the synsets according to
				// their rank.
				// Score= 1/2*first + 1/3*second + 1/4*third ..... etc.
				// Sum = 1/1 + 1/2 + 1/3 ...
				double score = 0.0;
				double sum = 0.0;
				for (Map.Entry<Integer, Double> setScore : synSetScoreMap
						.entrySet()) {
					score += setScore.getValue() / (double) setScore.getKey();
					sum += 1.0 / (double) setScore.getKey();
				}
				score /= sum;

				dictionary.put(word, score);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (csv != null) {
				csv.close();
			}
		}
	}
	
	@Override
	protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) 
			throws IOException, InterruptedException {
		String pathToSWN = "SentiWordNet.txt";
		SentiWordNetDemoCode(pathToSWN);
	}
	
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String[] col = value.toString().split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");
			
			if(col.length==27){//only accept properly formed data
				
				
				//Send Trustpoint
				if(!col[8].isEmpty() && !col[16].isEmpty()){
					t.set("TRUST:");
					v.set(String.valueOf(col[16])+":"+col[8]);
					context.write(t, v);
				}
				
				//Send Tweets
				if(!col[21].isEmpty()){
					String[] airlinelist = {"@united","@southwest","@jetblue","@virginamerica","@delta","@usairways","@americanair"};
					String[] searchlist = {"delayed","delay","miss"};
				
					
					String[] tweet = col[21].split(" ");
					Double sentiment = 0.0;
					
					//Loop through words of the tweet
					for (String word : tweet){
						if(dictionary.containsKey(word)){
							sentiment += (Double) dictionary.get(word);
						}
						
						for(String searchTerm: searchlist){
							if(word.trim().contains(searchTerm)){
								t.set("TWEET");
								v.set(searchTerm+":"+col[21]+":"+sentiment.toString());
								context.write(t, v);
							}
						}
						
						for(String airline: airlinelist){
							if(word.trim().contains(airline)){
								t.set("SENTIMENTBYAIRLINE");
								v.set(airline+":"+sentiment.toString());
								context.write(t, v);
							}
						}
					}
					
					
					if(!col[17].isEmpty() && sentiment!=0.0){
						
						t.set("SENTIWORD");
						String computed;
						String result;
						if(sentiment>0){
							computed = "positive";
						}
						else{
							computed = "negative";
						}
						
						if(col[17].trim().equalsIgnoreCase(computed)){
							result= "match";
						}
						else{
							result = "mismatch";
						}
						v.set(result+":"+col[17]+":"+sentiment.toString()+":"+computed);
						context.write(t, v);
					}
					
					
				}
			}

	}
	
}


