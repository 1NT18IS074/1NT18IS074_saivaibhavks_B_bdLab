package Cummulative_Award_package;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class CummulativeAward{
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
		//private final static IntWritable one = new IntWritable(1);
		private Text word = new Text("key");

		public void map(LongWritable key, Text value, Output<Collector<Text, IntWritable> output, Reporter reporter) throws IOException {
			String mstring = value.toString();// text to string
			String[] Tokens=mstring.split(",");
			if(Tokens[2].equals("30000"))
			output.collect(word,new IntWritable(Integer.parseInt(Tokens[3])));
			}
		}
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException { //{little: {1,1}} 
			int Awards=0;
			while(values.hasNext()) {
				Awards+=values.next().get();
			}
			output.collect(new Text("Total awards by the employees with salary 30000 : "), new IntWritable(Awards));
		}
	}
	

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(CummulativeAward.class);
		conf.setJobName("transaction amount of users");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		conf.setMapperClass(Map.class);
		conf.setCombinerClass(Reduce.class);
		conf.setReducerClass(Reduce.class);
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class); // hadoop jar jarname classpath inputfolder outputfolder
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);   
	}
		

	}
