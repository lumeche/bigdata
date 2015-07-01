package com.test.luis.hadoop.job.ch7;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.test.luis.hadoop.common.JobBuilder;
import com.test.luis.hadoop.io.input.WholeFileInputFormat;

public class WholeFileJob extends Configured implements Tool {

	@Override
	public int run(String[] arg0) throws Exception {
		Job job = JobBuilder.parseInputAndOutput(this, getConf(), arg0);
		if(job==null){
			return -1;
		}
			
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		
		job.setMapperClass(WholeFileMapper.class);
		job.setJarByClass(WholeFileJob.class);
		job.setNumReduceTasks(2);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new WholeFileJob(), args);
		System.exit(exitCode);
	}

	private static class WholeFileMapper extends
			Mapper<NullWritable, BytesWritable, Text,BytesWritable> {
		private Text filePath = new Text();
		private LongWritable LongWritable=new LongWritable();

		@Override
		protected void setup(
				Mapper<NullWritable, BytesWritable, Text,BytesWritable>.Context context)
				throws IOException, InterruptedException {
			InputSplit split = context.getInputSplit();
			Path path=((FileSplit)split).getPath();
			filePath=new Text(path.toString());
			LongWritable.set(new Random().nextLong());
		}

		@Override
		protected void map(
				NullWritable key,
				BytesWritable value,
				Mapper<NullWritable, BytesWritable, Text,BytesWritable>.Context context)
				throws IOException, InterruptedException {
			
			context.write(filePath,value);
		}
	}

}
