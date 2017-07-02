package com.mjyutika;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MaxTelephonesFinal {
	

	public static class ClasMapper extends Mapper<Text, Text, LongWritable, Text> {
		public void map(Text key, Text value, Context context ) throws
		IOException, InterruptedException {
		String[] tokens = value.toString().split("\t");
		int per100count;
			if (tokens[13].toString().equals("NA")) { 
					per100count = 0;
			} else {
				double d = Double.parseDouble(tokens[13].toString());
				 per100count = (int) d; }
				context.write(new LongWritable(per100count), key);
			}
	}
	
	public static class ClsCombiner extends Reducer<LongWritable, Text, LongWritable, Text> {
		int mCount = 0;
					
		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context) {
				if(mCount < 5) {
					try {
						for(Text value: values) {
							context.write(key, value);
							mCount++;
						}
					} catch(Exception e) {				
					}
				}
			}
	}
	
	public static class ClasReducer extends Reducer<LongWritable, Text, Text, LongWritable> {
		int mCount = 0;
		
		public void reduce(LongWritable key, Iterable<Text> values, Context context) {
			if(mCount < 5) {
				try {
					for(Text value: values) {
						context.write(value, key);
						mCount++;
					}
				} catch(Exception e) {
					
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		
		Configuration conf = new Configuration();
//		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", "\t");
		
		Job job = Job.getInstance(conf);
		
		job.setJobName("Top 5 highest telephone using states for year 2013 column");
		job.setJarByClass(MaxTelephonesFinal.class);
		
		job.setNumReduceTasks(1);
		
		job.setMapperClass(ClasMapper.class);
		job.setCombinerClass(ClsCombiner.class);
		job.setReducerClass(ClasReducer.class);
		job.setSortComparatorClass(LongWritable.DecreasingComparator.class);
		
		job.setInputFormatClass(KeyValueTextInputFormat.class);
		
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.submit();
		System.exit( job.waitForCompletion( true ) ? 0 : 1 );
	}


}//end of class MaxTelephones
