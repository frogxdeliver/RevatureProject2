package com.revature;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.revature.map.PercentageMapper;
import com.revature.reduce.PercentageReducer;



public class P2MaleEmployment {
	public static void main(String[] args) throws Exception{
		if(args.length != 2){
			System.out.println("WordCount usage: <input dir> <output dir>");
			System.exit(-1);
		}
		
		Job job = new Job();
		
		//Telling the job which class is the driver
		job.setJarByClass(P2MaleEmployment.class);
		
		job.setJobName("Project2 Male Employment");
		
		//Input and output paths
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//Specify the Mapper and Reducer class
		job.setMapperClass(PercentageMapper.class);
		job.setReducerClass(PercentageReducer.class);

		
		
		
		//don't forget to set the amount of reducers for Partitioning!
		job.setNumReduceTasks(1);
		
		
		//specify mapper outputs
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		//SPecify the type of the final output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class); //change later when we have reducers
		
		//check if the job is finished
		boolean success = job.waitForCompletion(true);
		System.exit(success ? 0 : 1);
		
	}
}
