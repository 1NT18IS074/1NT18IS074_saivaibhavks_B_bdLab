package my.pack.sai;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public class Prgm1 {

	//MAPPER CODE	
		   
	public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
			
		String myString= value.toString();
		String[] stuDetails= myString.split(",");
		if(Integer.parseInt(stuDetails[2])>60)
		output.collect(new Text("Total number of students who have scored more than 60 in subject 1"),new IntWritable( 1));
		
		}
	}

	//REDUCER CODE	
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException { 
		int totalStudents=0;
		while(values.hasNext()) {
			totalStudents += values.next().get();
		}
		output.collect(key, new IntWritable(totalStudents));
		}
	}
		
	//DRIVER CODE
	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(Prgm1.class);
		conf.setJobName("Total number of students who have scored more than 60 in subject 1");
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
