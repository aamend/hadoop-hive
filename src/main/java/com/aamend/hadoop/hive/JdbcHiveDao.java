package com.aamend.hadoop.hive;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.stereotype.Repository;

@Repository
public class JdbcHiveDao implements HiveDao {

	private JdbcOperations jdbcOperations;
	private int hiveLastPartitionRebuilt;
	private static Logger LOGGER = LoggerFactory.getLogger(JdbcHiveDao.class);

	@Override
	public void loadIacFile(String path) throws HiveException {

		String sql = "LOAD DATA INPATH '" + path + File.separator
				+ "' OVERWRITE INTO TABLE INTERNET_ACCESS_LOAD";
		LOGGER.debug("Executing sql [" + sql + "]");
		try {
			jdbcOperations.execute(sql);
		} catch (DataAccessException e) {
			String msg = "Could not load data to INTERNET_ACCESS_LOAD table ";
			LOGGER.error(msg + " : " + e.getMessage());
			throw new HiveException(msg, e);
		}

	}

	@Override
	public void partitionIacData() throws HiveException {

		String sql = "INSERT INTO TABLE internet_access "
				+ "PARTITION(iac_daily_partition) "
				+ "SELECT iac_stop_time, iac_start_time, "
				+ "iac_client_ip, iac_access_unit, "
				+ "iac_hostname, iac_input_bytes, "
				+ "iac_output_bytes, iac_apn, iac_session_id, "
				+ "iac_daily_partition FROM " + "internet_access_load";
		LOGGER.debug("Executing sql [" + sql + "]");
		try {
			jdbcOperations.execute(sql);
		} catch (DataAccessException e) {
			String msg = "Could not partition newly gathered data to INTERNET_ACCESS table ";
			LOGGER.error(msg + " : " + e.getMessage());
			throw new HiveException(msg, e);
		}

		sql = "SHOW PARTITIONS INTERNET_ACCESS";
		LOGGER.debug("Executing sql [" + sql + "]");
		try {

			// jdbcOperations.queryForList(sql, String.class);
			List<String> partitions = jdbcOperations.query(sql,
					new ResultSetExtractor<List<String>>() {
						@Override
						public List<String> extractData(ResultSet res)
								throws SQLException, DataAccessException {
							List<String> partitions = new ArrayList<String>();
							while (res.next()) {
								partitions.add(res.getString(1));
							}
							return partitions;
						}
					});

			LOGGER.info("Will now analyze / rebuild last "
					+ hiveLastPartitionRebuilt + " partitions / indexes");
			int count = partitions.size();
			for (int i = count - hiveLastPartitionRebuilt; i < count; i++) {
				String part = partitions.get(i);

//				LOGGER.info("Will now rebuild indexes on partition " + part);
//				sql = "ALTER index iac_ip_ix ON internet_access PARTITION("
//						+ part + ") REBUILD";
//				LOGGER.debug("Executing sql [" + sql + "]");
//				jdbcOperations.execute(sql);
//
//				LOGGER.info("Will now rebuild indexes on partition " + part);
//				sql = "ALTER index iac_msisdn_ix ON internet_access PARTITION("
//						+ part + ") REBUILD";
//				LOGGER.debug("Executing sql [" + sql + "]");
//				jdbcOperations.execute(sql);

				LOGGER.info("Will now analyse partition " + part);
				sql = "ANALYZE TABLE internet_access " + "PARTITION(" + part
						+ ") " + "COMPUTE STATISTICS";
				LOGGER.debug("Executing sql [" + sql + "]");
				jdbcOperations.execute(sql);
			}

		} catch (DataAccessException e) {
			String msg = "Could not list and analyze partitions from INTERNET_ACCESS table ";
			LOGGER.error(msg + " : " + e.getMessage());
			throw new HiveException(msg, e);
		}

	}

	@Override
	public List<InternetAccess> getIac(InternetAccessService.TargetType targetType,
			String targetValue, String strFromTimeStamp, String strToTimeStamp,
			String strFromDate, String strToDate) throws HiveException {

		if (strFromTimeStamp == null || strFromTimeStamp.isEmpty()
				|| strToTimeStamp == null || strToTimeStamp.isEmpty()
				|| strFromDate == null || strFromDate.isEmpty()
				|| strToDate == null || strToDate.isEmpty()) {
			throw new HiveException(
					"Could not fetch result with empty startDate or StopDate");
		}

		String sql = "select distinct iac_stop_time,  "
				+ "iac_start_time,iac_client_ip, "
				+ "iac_access_unit, iac_hostname, iac_input_bytes, "
				+ "iac_output_bytes, iac_apn, "
				+ "iac_session_id from internet_access where "
				+ targetType.getValue() + " = '" + targetValue + "'"
				+ " and iac_stop_time >= '" + strFromTimeStamp + "'"
				+ " and iac_stop_time <= '" + strToTimeStamp + "'"
				+ " and iac_daily_partition >= " + strFromDate + ""
				+ " and iac_daily_partition <= " + strToDate + "";
		LOGGER.info("Executing sql : [" + sql + "]");
		List<InternetAccess> iacs;
		try {
			iacs = jdbcOperations.query(sql,
					new ResultSetExtractor<List<InternetAccess>>() {
						@Override
						public List<InternetAccess> extractData(ResultSet res)
								throws SQLException, DataAccessException {

							List<InternetAccess> list = new ArrayList<InternetAccess>();
							while (res.next()) {
								String stopTime = res.getString(1);
								String startTime = res.getString(2);
								String clientIp = res.getString(3);
								String accessUnit = res.getString(4);
								String hostname = res.getString(5);
								String inputBytes = res.getString(6);
								String outputBytes = res.getString(7);
								String apn = res.getString(8);
								String sessionId = res.getString(9);
								list.add((InternetAccess) new InternetAccessEntity(
										stopTime, startTime, clientIp,
										accessUnit, hostname, inputBytes,
										outputBytes, apn, sessionId));
							}

							return list;
						}
					});

		} catch (DataAccessException e) {
			String msg = "Could not fetch data from INTERNET_ACCESS table ";
			LOGGER.error(msg + " : " + e.getMessage());
			throw new HiveException(msg, e);
		}
		return iacs;
	}

	@Value("${hive.last.part.rebuild}")
	public void setHiveLastPartitionRebuilt(int hiveLastPartitionRebuilt) {
		this.hiveLastPartitionRebuilt = hiveLastPartitionRebuilt;
	}

	@Autowired
	public void setJdbcOperations(JdbcOperations jdbcOperations) {
		this.jdbcOperations = jdbcOperations;
	}

}
