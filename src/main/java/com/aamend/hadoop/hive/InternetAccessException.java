package com.aamend.hadoop.hive;


@SuppressWarnings("serial")
public class InternetAccessException extends Exception {

	public InternetAccessException(String msg) {
		super(msg);
	}

	public InternetAccessException(String msg, Exception e) {
		super(msg,e);
	}
	
}
