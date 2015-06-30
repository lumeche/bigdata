package com.test.luis.hadoop.io.input;

import java.io.IOException;





import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
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
			Path path = fileSplit.getPath();
			logger.debug("About to read file %s", path.toString());
			FileSystem fileSystem = path.getFileSystem(conf);
			try {
				FSDataInputStream is=fileSystem.open(path);
				IOUtils.readFully(is, content, 0, content.length);
				value.set(content, 0, content.length);
				return true;
			} catch (Exception e) {
				logger.error("Error reading file",e);;
				return false;
			}
		}
	}

	
	@Override
	public NullWritable getCurrentKey() throws IOException,
			InterruptedException {
		return NullWritable.get();
	}

	@Override
	public BytesWritable getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}
	
	@Override
	public void close() throws IOException {
		logger.debug("Doing nothing in close");
		
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return processed?1.0f:0.0f;
	}

	


}
