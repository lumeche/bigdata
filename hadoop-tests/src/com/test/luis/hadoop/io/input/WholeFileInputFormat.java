package com.test.luis.hadoop.io.input;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WholeFileInputFormat extends FileInputFormat<NullWritable, ByteWritable>{

	private static final Logger logger=LogManager.getLogger(WholeFileInputFormat.class);
			
	@Override
	protected boolean isSplitable(FileSystem fs, Path filename) {
		logger.debug("Returning false to isSplitable");
		return false;
	}

	@Override
	public RecordReader<NullWritable, ByteWritable> getRecordReader(
			InputSplit arg0, JobConf arg1, Reporter arg2) throws IOException {
		// TODO Auto-generated method stub
		return null;
	}

}
