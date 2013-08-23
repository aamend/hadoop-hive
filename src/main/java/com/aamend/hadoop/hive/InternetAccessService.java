package com.aamend.hadoop.hive;

import java.io.ByteArrayInputStream;

import org.joda.time.DateTime;


public interface InternetAccessService {

	public enum TargetType {

		IP("iac_client_ip"), MSISDN("iac_access_unit");

		private String value;

		private TargetType(String value) {
			this.value = value;
		}

		public String getValue() {
			return value;
		}
	}

	/**
	 * Search Internet Access from IP target
	 * @param ip address to look for
	 * @param fromDate Timestamp
	 * @param toDate Timestamp
	 * @return ByteArray of CSV
	 * @throws InternetAccessException
	 */
	ByteArrayInputStream searchByIp(String ip, DateTime fromDate, DateTime toDate)
			throws InternetAccessException;

	/**
	 * Search Internet Access from MSISDN target
	 * @param msisdn to look for
	 * @param fromDate Timestamp
	 * @param toDate Timestamp
	 * @return ByteArray of CSV
	 * @throws InternetAccessException
	 */
	ByteArrayInputStream searchByMsisdn(String msisdn, DateTime fromDate, DateTime toDate)
			throws InternetAccessException;

	/**
	 * Load data from HDFS to Hive temporary table
	 * Partition data into final table
	 * @throws InternetAccessException
	 */
	void loadData() throws InternetAccessException;

}
