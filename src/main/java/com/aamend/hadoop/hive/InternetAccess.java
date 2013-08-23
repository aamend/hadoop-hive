package com.aamend.hadoop.hive;

public interface InternetAccess {

	String getStartTime();
	String getStopTime();
	String getClientIp();
	String getAccessUnit();
	String getHostname();
	String getInputBytes();
	String getOutputBytes();
	String getCalledStId();
	String getSessionId();
}
