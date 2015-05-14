package com.test.luis.hadoop.io.input;

import java.io.IOException;
import java.nio.file.Files;

import org.apache.commons.configuration.Configuration;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WholeFileReader extends RecordReader<NullWritable, BytesWritable>{
	
	private static final Logger logger = LogManager.getLogger(WholeFileReader.class);
	private BytesWritable value=new BytesWritable();
	private boolean processed=false;
	private FileSplit fileSplit;
	private org.apache.hadoop.conf.Configuration conf;
	@Override
	public void initialize(InputSplit inputSplit, TaskAttemptContext context)
			throws IOException, InterruptedException {
		fileSplit=(FileSplit) inputSplit;
		conf=context.getConfiguration();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if(processed){
			return false;
		}else{
			byte[] content=new byte[(int) fileSplit.getLength()];
			
		}
	}

	
	@Override
	public NullWritable getCurrentKey() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return NullWritable.get();
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException,
			InterruptedException {
		// TODO Auto-generated method stub
		return value;
	}
	
	@Override
	public void close() throws IOException {
		logger.debug("Doing nothing in close");
		
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		return 0;
	}

	


}
