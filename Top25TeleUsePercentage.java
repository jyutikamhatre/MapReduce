package com.mjyutika;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
 
 public class Top25TeleUsePercentage {
 public static class StatesUseMapper extends Mapper <Object, Text, Text, Text>
 {
 public void map(Object key, Text value, Context context)
 throws IOException, InterruptedException 
 {
 String record = value.toString();
 String[] parts = record.split("\t");
 for (int i=1; i<parts.length; i++)
 {
	 if (!parts[i].toString().equals("NA"))
	 {
		 String year = "";
		 switch (i)
		 {			
			case 5:
				year = "2004";
				break;
			case 6:
				year = "2005";
				break;
			case 7:
				year = "2006";
				break;
			case 8:
				year = "2007";
				break;
			case 9:
				year = "2008";
				break;
			case 10:
				year = "2009";
				break;
			case 11:
				year = "2010";
				break;
			case 12:
				year = "2011";
				break;
			case 13:
				year = "2012";
				break;
			case 14:
				year = "2013";
				break;
			default :
				year = "NotAvailable";
				break;
				}
	 context.write(new Text(year), new Text("st\t" + parts[0] +"\t" + parts[i]));
	 }
 }
 }
 }
 
 public static class YearTotalMapper extends Mapper <Object, Text, Text, Text>
 {
 public void map(Object key, Text value, Context context) 
 throws IOException, InterruptedException 
 {
 String record = value.toString();
 String[] parts = record.split("\t");
 context.write(new Text(parts[0]), new Text("yr\t" + parts[3]));
 }
 }
 
 public static class JoinReducer extends Reducer <Text, Text, Text, Text>
 {
 public void reduce(Text key, Iterable<Text> values, Context context)
 throws IOException, InterruptedException 
 {
	 String yrTotal = "";
	 for (Text t : values)
	 {
		 String parts[] = t.toString().split("\t");
	 if( parts[0].equals("yr"))
			 {
		 		yrTotal = parts[1];
			 }
	 context.write(key, new Text(t.toString() + "\t" + yrTotal));
	 }
	
// context.write(key,t);
  
 }
 }
 
 public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 Job job = new Job(conf, "Yearly Percentage used");
 job.setJarByClass(Top25TeleUsePercentage.class);
 job.setReducerClass(JoinReducer.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(Text.class);
  
 MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class, StatesUseMapper.class);
 MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class, YearTotalMapper.class);
 Path outputPath = new Path(args[2]);
  
 FileOutputFormat.setOutputPath(job, outputPath);
 outputPath.getFileSystem(conf).delete(outputPath);
 System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
 }