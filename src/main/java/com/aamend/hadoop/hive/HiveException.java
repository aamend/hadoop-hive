package com.aamend.hadoop.hive;


@SuppressWarnings("serial")
public class HiveException extends Exception {

	public HiveException(String msg) {
		super(msg);
	}

	public HiveException(String msg, Exception e) {
		super(msg,e);
	}
	
}
