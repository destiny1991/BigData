package edu.buaa.MapReduce;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordCount {
	static final String INPUT_PATH = "hdfs://parallels:9000/mapreduce/input/";
	static final String OUTPUT_PATH = "hdfs://parallels:9000/mapreduce/output/";

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
		Configuration conf = new Configuration();
		
		//if the output dir exits, then delete it.
		FileSystem fileSystem = FileSystem.get(new URI(OUTPUT_PATH),conf);
		Path outpath = new Path(OUTPUT_PATH);
		if(fileSystem.exists(outpath))
			fileSystem.delete(outpath, true);
		
		Job job = Job.getInstance(conf, "WordCount");
		
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		FileOutputFormat.setOutputPath(job, outpath);
	
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		job.waitForCompletion(true);
	}
	
	static class MyMapper extends 
		Mapper<LongWritable, Text, Text, LongWritable> {
		
		@Override
		public void map(LongWritable k1, Text v1, Context context)
				throws IOException, InterruptedException {
			String[] splits = v1.toString().split(" ");
			for(String str: splits) {
				context.write(new Text(str), new LongWritable(1));
			}
		}
	}
	
	static class MyReducer extends
		Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		public void reduce(Text k2, Iterable<LongWritable> v2s, Context context)
				throws IOException, InterruptedException {
			long tmp = 0;
			for(LongWritable v2: v2s) {
				tmp += v2.get();
			}
			context.write(k2, new LongWritable(tmp));
		}
	}
}
