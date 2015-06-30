package com.test.luis.hadoop.job.ch7;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.util.*;

import com.test.luis.hadoop.common.JobBuilder;
import com.test.luis.hadoop.io.input.WholeFileInputFormat;

public class WholeFileJob extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		Job job=JobBuilder.parseInputAndOutput(this, getConf(), arg0);
		
		job.setMapperClass(WholeFileMapper.class);
		
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setMapOutputKeyClass(LongWritable.class);
		
		job.setMapOutputValueClass(Text.class);
		
		job.setPartitionerClass(HashPartitioner.class);
		job.setNumReduceTasks(1);
		job.setReducerClass(org.apache.hadoop.mapreduce.Reducer.class);
		
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(Text.class);
		
		job.setOutputFormatClass(TextOutputFormat.class);
		
		return job.waitForCompletion(true)?0:1;
	}

	public static void main(String[] args) throws Exception{
		int exitCode =ToolRunner.run(new WholeFileJob(), args);
		System.exit(1);
	}

	private static class WholeFileMapper extends
			Mapper<NullWritable, ByteWritable, Text, ByteWritable> {
		private Text filePath = new Text();

		@Override
		protected void setup(
				Mapper<NullWritable, ByteWritable, Text, ByteWritable>.Context context)
				throws IOException, InterruptedException {
			FileSplit split = (FileSplit) context.getInputSplit();
			filePath.set(split.getPath().toString());
		}

		@Override
		protected void map(
				NullWritable key,
				ByteWritable value,
				Mapper<NullWritable, ByteWritable, Text, ByteWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(filePath, value);
		}
	}

}
