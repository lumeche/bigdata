package com.test.luis.hadoop.io.input;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WholeFileInputFormat extends FileInputFormat<NullWritable, ByteWritable>{
	
	private static final Logger logger=LogManager.getLogger(WholeFileInputFormat.class);
			
	protected boolean isSplitable(FileSystem fs, Path filename) {
		logger.debug("Returning false to isSplitable");
		return false;
	}

	
	@Override
	public RecordReader<NullWritable, ByteWritable> createRecordReader(
			org.apache.hadoop.mapreduce.InputSplit arg0, TaskAttemptContext arg1)
			throws IOException, InterruptedException {
		RecordReader recordReader = new WholeFileReader();
		recordReader.initialize(arg0, arg1);
		return recordReader;
	}

}
