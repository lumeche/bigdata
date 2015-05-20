package com.test.luis.hadoop.job.ch7;



import java.io.IOException;

import javax.lang.model.type.NullType;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;


public class WholeFileJob {

	private static class WholeFileMapper extends
			Mapper<NullType, ByteWritable, Text, ByteWritable> {
		private Text filePath = new Text();

		@Override
		protected void setup(
				Mapper<NullType, ByteWritable, Text, ByteWritable>.Context context)
				throws IOException, InterruptedException {
			FileSplit split = (FileSplit) context.getInputSplit();
			filePath.set(split.getPath().toString());
		}

		@Override
		protected void map(
				NullType key,
				ByteWritable value,
				Mapper<NullType, ByteWritable, Text, ByteWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(filePath, value);
		}		
	}

}
