package com.aamend.hadoop.hive;


public class InternetAccessEntity implements InternetAccess {

	public String stopTime;
	public String startTime;
	public String clientIp;
	public String accessUnit;
	public String hostname;
	public String inputBytes;
	public String outputBytes;
	public String calledStId;
	public String sessionId;

	public InternetAccessEntity(String stopTime, String startTime, String clientIp,
			String accessUnit, String hostname, String inputBytes,
			String outputBytes, String calledStId, String sessionId) {

		this.stopTime = stopTime;
		this.startTime = startTime;
		this.clientIp = clientIp;
		this.accessUnit = accessUnit;
		this.hostname = hostname;
		this.inputBytes = inputBytes;
		this.outputBytes = outputBytes;
		this.calledStId = calledStId;
		this.sessionId = sessionId;

	}

	public String getStopTime() {
		return stopTime;
	}

	public void setStopTime(String stopTime) {
		this.stopTime = stopTime;
	}

	public String getStartTime() {
		return startTime;
	}

	public void setStartTime(String startTime) {
		this.startTime = startTime;
	}

	public String getClientIp() {
		return clientIp;
	}

	public void setClientIp(String clientIp) {
		this.clientIp = clientIp;
	}

	public String getAccessUnit() {
		return accessUnit;
	}

	public void setAccessUnit(String accessUnit) {
		this.accessUnit = accessUnit;
	}

	public String getHostname() {
		return hostname;
	}

	public void setHostname(String hostname) {
		this.hostname = hostname;
	}

	public String getInputBytes() {
		return inputBytes;
	}

	public void setInputBytes(String inputBytes) {
		this.inputBytes = inputBytes;
	}

	public String getOutputBytes() {
		return outputBytes;
	}

	public void setOutputBytes(String outputBytes) {
		this.outputBytes = outputBytes;
	}

	public String getCalledStId() {
		return calledStId;
	}

	public void setCalledStId(String calledStId) {
		this.calledStId = calledStId;
	}

	public String getSessionId() {
		return sessionId;
	}

	public void setSessionId(String sessionId) {
		this.sessionId = sessionId;
	}

}
