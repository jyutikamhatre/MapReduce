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

public class Top25TeleStatesAcrossYears {
	

	public static class ClasMapper extends Mapper<Text, Text, LongWritable, Text> {
		public void map(Text key, Text value, Context context ) throws
		IOException, InterruptedException {
		String[] tokens = value.toString().split("\t");
		int per100count=0;
		String year = "";
		for (int i=1; i<tokens.length; i++){
			if (tokens[i].toString().equals("NA")) { 
					per100count = 0;
			} else {
				double d = Double.parseDouble(tokens[i].toString());
				 per100count = (int) d; }
			
			switch (i){			
			case 1:
				year = "2001";
				break;
			case 2:
				year = "2002";
				break;
			case 3:
				year = "2003";
				break;
			case 4:
				year = "2004";
				break;
			case 5:
				year = "2005";
				break;
			case 6:
				year = "2006";
				break;
			case 7:
				year = "2007";
				break;
			case 8:
				year = "2008";
				break;
			case 9:
				year = "2009";
				break;
			case 10:
				year = "2010";
				break;
			case 11:
				year = "2011";
				break;
			case 12:
				year = "2012";
				break;
			case 13:
				year = "2013";
				break;
			default :
				break;
				}
			
			context.write(new LongWritable(per100count),new Text(key+"\t"+year));
			}//end of for loop
		}
	}
	
	public static class ClsCombiner extends Reducer<LongWritable, Text, LongWritable, Text> {
		int mCount = 0;
					
		@Override
		public void reduce(LongWritable key, Iterable<Text> values, Context context) {
				if(mCount < 25) {
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
			if(mCount < 25) {
				try {
				//	int j = 1;
					for(Text value: values) {
						context.write(value, key);
				//		context.write(new Text(j+"\t"+value), key);
						mCount++;
				//		j++;
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
		
		job.setJobName("Top 25 telephone using state across all years");
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
