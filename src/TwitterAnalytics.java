import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException; 
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
		Path inputPath = new Path("hdfs://localhost:9000/user/project/input/"); 
		Path outputPath = new Path("hdfs://localhost:9000/user/project/temp/");
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
		inputPath = new Path("hdfs://localhost:9000/user/project/temp/"); 
		outputPath = new Path("hdfs://localhost:9000/user/project/output/");
		outputPath.getFileSystem(conf).delete(outputPath, true);
		FileInputFormat.addInputPath(analysisJob, inputPath);
		FileOutputFormat.setOutputPath(analysisJob, outputPath);
		
		analysisJob.waitForCompletion(true);
		
		
		/*
		Job tweetJob = Job.getInstance(conf, "TwitterAnalytics"); 
		tweetJob.setJarByClass(TwitterAnalytics.class); 
		tweetJob.setMapperClass(TweetMapper.class);
		tweetJob.setReducerClass(TweetReducer.class); 
		tweetJob.setOutputKeyClass(Text.class); 
		tweetJob.setOutputValueClass(Text.class); 
		inputPath = new Path("hdfs://localhost:9000/user/project/input/"); 
		outputPath = new Path("hdfs://localhost:9000/user/project/tweets/");
		outputPath.getFileSystem(conf).delete(outputPath, true);
		FileInputFormat.addInputPath(tweetJob, inputPath);
		FileOutputFormat.setOutputPath(tweetJob, outputPath);
		
		tweetJob.waitForCompletion(true);
		*/

	} 
}

