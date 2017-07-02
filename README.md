# MapReduce
Proof of concept - Hadoop - MapReduce



1. Map Reduce

a. Finding top 5 states for one particular year i.e. 2013

Project Description
Finding the top 5 states using telephones for year 2013
Dataset Resource
https://data.gov.in/catalog/state-wise-telephones-statistics-100-population-india
Special operations
1.choosing only one particular column 2.eliminating null values
3.converting double number to integer
Goal
To identify which are the top 5 states in which number of telephones used per 100 people is greater and list them in descending order. This is for year 2013.
Jar file
maxTele2013.jar
Java class
MaxTelephonesFinal.java
Input file
/input/Telephones.txt
Output file
/output/Top5Tele

Input File: Telephones.txt

All-India	2.86	3.53	4.29	5.11	7.08	8.95	12.74	18.22	26.22	36.98	52.74	70.89	78.66	73.32
Andaman & Nicobar	6.34	8.45	9.04	10.15	11.56	12.63	17.97	17.39	18.36	21.24	29.96	NA	NA	NA
Andhra Pradesh	3.13	4.10	4.93	5.66	7.85	9.48	13.45	19.62	28.25	39.59	57.23	74.35	80.87	77.19
Assam	1.06	1.33	1.67	1.94	2.13	2.79	5.67	9.74	14.74	20.65	29.99	38.98	46.61	46.51
Bihar	0.65	1.15	1.08	1.32	1.67	2.36	5.34	7.32	12.64	22.18	37.96	42.32	48.90	45.72
Chhattisgarh	NA	NA	1.25	1.47	1.63	1.80	2.09	3.24	4.38	5.15	5.74	NA	NA	NA
Gujarat	4.26	5.37	6.37	7.77	10.14	12.73	16.98	24.14	33.63	45.16	58.46	81.9	91.13	87.23
Haryana	3.36	4.25	5.06	6.21	8.38	10.83	14.47	23.11	30.39	43.75	59.70	82.59	89.42	76.44
Himachal Pradesh	4.32	5.31	7.48	8.50	10.14	13.12	18.78	28.57	41.16	55.50	79.35	111.11	120.68	105.39
Jammu & Kashmir	1.31	1.72	2.15	2.48	3.01	5.09	12.18	16.08	21.84	32.76	49.91	NA	NA	NA
Jharkhand	NA	NA	1.39	1.68	2.00	2.30	2.99	3.43	3.60	4.11	5.54	NA	NA	NA
Karnataka	3.76	4.70	5.58	6.67	9.46	12.19	17.06	25.05	34.53	45.21	67.81	87.76	97.22	91.24
Kerala	5.60	7.51	9.51	11.33	14.87	18.77	25.54	33.54	45.34	58.48	80.36	100.01	106.61	96.09
Madhya Pradesh	1.54	1.81	2.49	3.02	3.99	5.21	7.12	12.22	20.29	30.08	45.23	48.88	53.81	53.55
Maharashtra	5.40	6.60	5.14	6.08	8.00	10.01	13.10	18.78	27.42	37.90	50.30	68.97	77.19	73.97
North East-I	1.56	1.92	2.41	3.00	3.35	4.33	8.11	16.56	27.67	44.49	68.90	56.5	65.72	67.78
North East-II	NA	NA	NA	2.35	2.71	3.66	5.21	7.41	9.14	9.21	11.91	NA	NA	NA
Orissa	1.21	1.52	1.88	2.29	2.95	3.96	7.57	9.51	15.00	23.30	39.30	56.37	65.84	60.21
Punjab	5.67	6.95	9.15	11.76	17.33	21.94	27.61	37.05	47.89	58.25	75.44	104.09	113.13	103.00
Rajasthan	2.11	2.57	3.02	3.47	4.50	6.12	9.65	15.49	23.74	37.15	52.76	65.35	72.96	70.85
Tamil Nadu	4.52	5.91	5.37	6.22	8.54	11.37	14.70	22.55	35.09	50.46	74.31	97.73	116.61	108.17
Uttarakhand	NA	NA	3.64	4.25	5.10	5.74	7.46	9.50	10.61	11.59	13.90	NA	NA	NA
Uttar Pradesh-(E&W)	1.33	1.66	1.86	2.15	2.96	4.06	6.87	10.77	16.19	24.91	38.54	52.97	60.93	56.83
West Bengal	2.09	2.67	1.52	1.85	2.18	3.00	5.53	8.63	14.36	22.51	34.81	53.43	61.52	54.18
Kolkata	NA	NA	11.77	13.47	18.92	23.79	33.70	45.09	64.22	89.68	120.19	163.76	172.22	145.86
Chennai	NA	NA	19.72	22.97	38.81	46.76	61.08	75.46	103.90	127.38	149.42	163.41	NA	NA
Delhi	15.40	17.66	22.11	27.38	41.79	50.94	65.40	86.89	110.05	140.18	172.49	225.26	238.59	221.64
Mumbai	NA	NA	20.27	24.22	36.08	44.27	56.73	64.99	83.48	110.52	143.71	180.45	183.52	152.44

MaxTelephonesFinal.java

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

Output:

notroot@ubuntu:~/lab/programs$ hdfs dfs -cat /output/Top5Tele/part*
Delhi   221
Mumbai  152
Kolkata 145
Tamil Nadu      108
Himachal Pradesh        105


b. Finding top 5 records across the years


Project Description
Finding the top 5 states using highest number of telephones for years through 2000 to 2013
Dataset Resource
https://data.gov.in/catalog/state-wise-telephones-statistics-100-population-india
Special operations
1. choosing all columns
2. switch case
3.modifying output using concatenation
Goal
To identify which are the top 5 states in which number of telephones used per 100 people is greater and list them in descending order. This is across all years from 2000 to 2013.
Jar file
highestTele.jar
Java class
HighestTeleAllColmns.java
Input file
/input/Telephones.txt ( same as 1.a)
Output file
/output/highestTeleUse

HighestTeleAllColmns.java

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

public class HighestTeleAllColmns {
	

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
		
		job.setJobName("Highest telephone using state across years");
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


Output :

notroot@ubuntu:~/lab/programs$ hdfs dfs -cat /output/highestTeleUse/part*       
Delhi   2012    238
Delhi   2011    225
Delhi   2013    221
Mumbai  2012    183
Mumbai  2011    180

c. Finding top 25 states across the years with rank

Project Description
Finding the top 25 states using highest number of telephones for years through 2000 to 2013
Dataset Resource
https://data.gov.in/catalog/state-wise-telephones-statistics-100-population-india
Special operations
Modifying code for 25 rows
Goal
To identify which are the top 25 states in which number of telephones used per 100 people is greater and list them in descending order. This is across all years from 2000 to 2013.
Jar file
top25Tele.jar
Java class
Top25TeleStatesAcrossYears.java
Input file
/input/Telephones.txt ( same as 1.a)
Output file
/output/top25Tele


Top25TeleStatesAcrossYears.java

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

Output : 

notroot@ubuntu:~/lab/programs$ hdfs dfs -cat /output/top25Tele/part*
Delhi   2012    238
Delhi   2011    225
Delhi   2013    221
Mumbai  2012    183
Mumbai  2011    180
Delhi   2010    172
Kolkata 2012    172
Kolkata 2011    163
Chennai 2011    163
Mumbai  2013    152
Chennai 2010    149
Kolkata 2013    145
Mumbai  2010    143
Delhi   2009    140
Chennai 2009    127
Himachal Pradesh        2012    120
Kolkata 2010    120
Tamil Nadu      2012    116
Punjab  2012    113
Himachal Pradesh        2011    111
Mumbai  2009    110
Delhi   2008    110
Tamil Nadu      2013    108
Kerala  2012    106
Himachal Pradesh        2013    105

d. Finding top 25 states comparing % usage with total use of that particular year


Project Description
Finding the top 25 states with % usage of number of telephones out of total use of that particular year for years through 2000 to 2013
Dataset Resource
https://data.gov.in/catalog/state-wise-telephones-statistics-100-population-india

https://data.gov.in/catalog/number-telephones-india-year-wise
Special operation
1.Read 2 datasets
2.Calculate % use of states yearwise ( like JOIN)
Goal
To identify which are the top 25 states in which number of telephones used per 100 people is greater and comparing its use with total usage for that year , list them in descending order. This is across all years from 2000 to 2013.
Jar file
telePercentage.jar
Java class
Top25TeleUsePercentage.java
Input file
/input/Telephones.txt ( same as 1.a)
/input/TotalTelephonesYearly.txt
Output file
/output/telePercentage

Input file: TotalTelephonesYearly.txt

2004	40.92	35.61	76.53
2005	41.42	56.95	98.37
2006	40.23	101.86	142.09
2007	40.77	165.09	205.87
2008	39.41	261.08	300.49
2009	37.97	391.76	429.73
2010	36.96	584.32	621.28
2011	34.73	811.59	846.32
2012	32.17	919.17	951.35
2013	30.21	867.81	898.02

Top25TeleUsePercentage.java

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

Output : ( sample rows)

2007    yr      205.87  205.87
2008    yr      300.49  300.49
2008    st      Kerala  45.34   300.49
2008    st      All-India       26.22   300.49
2008    st      Delhi   110.05  300.49
2008    st      Gujarat 33.63   300.49
2008    st      Mumbai  83.48   300.49
2008    st      Karnataka       34.53   300.49
2008    st      Orissa  15.00   300.49
2008    st      Tamil Nadu      35.09   300.49
2008    st      Uttar Pradesh-(E&W)     16.19   300.49
2008    st      Andhra Pradesh  28.25   300.49
2008    st      North East-II   9.14    300.49
2008    st      Andaman & Nicobar       18.36   300.49
2008    st      West Bengal     14.36   300.49
2008    st      Rajasthan       23.74   300.49
2008    st      Punjab  47.89   300.49
2008    st      North East-I    27.67   300.49
2008    st      Jammu & Kashmir 21.84   300.49
2008    st      Bihar   12.64   300.49
2008    st      Uttarakhand     10.61   300.49
2008    st      Maharashtra     27.42   300.49
2008    st      Kolkata 64.22   300.49
2008    st      Jharkhand       3.60    300.49
2008    st      Assam   14.74   300.49
2008    st      Madhya Pradesh  20.29   300.49
2008    st      Haryana 30.39   300.49
2008    st      Chennai 103.90  300.49
2008    st      Himachal Pradesh        41.16   300.49
2008    st      Chhattisgarh    4.38    300.49
2009    st      Punjab  58.25
2009    st      Himachal Pradesh        55.50

