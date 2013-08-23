package com.aamend.hadoop.hdfs;


@SuppressWarnings("serial")
public class HDFSException extends Exception {

	public HDFSException(String msg) {
		super(msg);
	}

	public HDFSException(String msg, Exception e) {
		super(msg,e);
	}
	
}
