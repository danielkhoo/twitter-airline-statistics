/*
 	* Class name: TwitterAnalytics (Driver Class)
 	* 
 	* Done by: Daniel
 	* 
 	* Description:
 	* This is the main class for our application, 
 	* it configures and runs the 3 map/reduces jobs
 	* for all our tasks
 	* 
*/
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Date; 

import org.apache.hadoop.conf.Configuration; 
import org.apache.hadoop.fs.Path; 
import org.apache.hadoop.io.IntWritable; 
import org.apache.hadoop.io.Text; 

import org.apache.hadoop.mapreduce.Job; 
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class TwitterAnalytics { 

	public static void main(String[] args) throws Exception {  
		Configuration conf = new Configuration(); 
		
		Job job = Job.getInstance(conf, "TwitterAnalytics"); 
		job.setJarByClass(TwitterAnalytics.class); 
		job.setMapperClass(RawDataMapper.class);
		job.setReducerClass(RawDataReducer.class); 
		job.setOutputKeyClass(Text.class); 
		job.setOutputValueClass(IntWritable.class); 
		//Path inputPath = new Path("hdfs://localhost:9000/user/project/input/"); 
		//Path outputPath = new Path("hdfs://localhost:9000/user/project/temp/");
		Path inputPath = new Path("oss://ict2107-daniel/input/");
		Path outputPath = new Path("oss://ict2107-daniel/temp/");
		outputPath.getFileSystem(conf).delete(outputPath, true);
		FileInputFormat.addInputPath(job, inputPath);
		FileOutputFormat.setOutputPath(job, outputPath);
		
		job.waitForCompletion(true);
		
		Job analysisJob = Job.getInstance(conf, "TwitterAnalytics"); 
		analysisJob.setJarByClass(TwitterAnalytics.class);
		analysisJob.setMapperClass(AnalysisMapper.class);
		analysisJob.setReducerClass(AnalysisReducer.class); 
		analysisJob.setOutputKeyClass(Text.class); 
		analysisJob.setOutputValueClass(Text.class); 
		
		String id = String.valueOf(new Date().getTime());
		//inputPath = new Path("hdfs://localhost:9000/user/project/temp/"); 
		//outputPath = new Path("hdfs://localhost:9000/user/project/output/");
		//analysisJob.addCacheFile(new URI("hdfs://localhost:9000/user/project/ISO-3166-alpha3.tsv"));
		inputPath = new Path("oss://ict2107-daniel/temp/");
		outputPath = new Path("oss://ict2107-daniel/output/");
		analysisJob.addCacheFile(new URI("oss://ict2107-daniel/ISO-3166-alpha3.tsv"));
		outputPath.getFileSystem(conf).delete(outputPath, true);
		FileInputFormat.addInputPath(analysisJob, inputPath);
		FileOutputFormat.setOutputPath(analysisJob, outputPath);
		
		analysisJob.waitForCompletion(true);
		
		
		
		Job tweetJob = Job.getInstance(conf, "TwitterAnalytics"); 
		tweetJob.setJarByClass(TwitterAnalytics.class); 
		tweetJob.setMapperClass(TweetMapper.class);
		tweetJob.setReducerClass(TweetReducer.class); 
		tweetJob.setOutputKeyClass(Text.class); 
		tweetJob.setOutputValueClass(Text.class); 
		//inputPath = new Path("hdfs://localhost:9000/user/project/input/"); 
		//tweetJob.addCacheFile(new URI("hdfs://localhost:9000/user/project/SentiWordNet.txt"));
		//outputPath = new Path("hdfs://localhost:9000/user/project/tweets/");
		inputPath = new Path("oss://ict2107-daniel/input/");
		tweetJob.addCacheFile(new URI("oss://ict2107-daniel/SentiWordNet.txt"));
		outputPath = new Path("oss://ict2107-daniel/tweets/");
		outputPath.getFileSystem(conf).delete(outputPath, true);
		FileInputFormat.addInputPath(tweetJob, inputPath);
		FileOutputFormat.setOutputPath(tweetJob, outputPath);
		
		tweetJob.waitForCompletion(true);
		

	} 
}

