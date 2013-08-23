package com.aamend.hadoop.hive;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.aamend.hadoop.hdfs.HDFSException;
import com.aamend.hadoop.hdfs.HDFSService;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class InternetAccessServiceBean implements InternetAccessService {

	private String hdfsDir;
	private String localDir;

	DateTimeFormatter fmtDay = DateTimeFormat.forPattern("yyyyMMdd");
	DateTimeFormatter fmtTimeStamp = DateTimeFormat
			.forPattern("yyyyMMddHHmmss");

	private static Logger LOGGER = LoggerFactory
			.getLogger(InternetAccessServiceBean.class);

	private HiveDao hiveDao;
	private HDFSService hdfsClient;

	@Override
	public ByteArrayInputStream searchByIp(String ip, DateTime fromDate,
			DateTime toDate) throws InternetAccessException {
		return searchByTargetType(TargetType.IP, ip, fromDate, toDate);
	}

	@Override
	public ByteArrayInputStream searchByMsisdn(String msisdn,
			DateTime fromDate, DateTime toDate) throws InternetAccessException {
		return searchByTargetType(TargetType.MSISDN, msisdn, fromDate, toDate);
	}

	private ByteArrayInputStream searchByTargetType(TargetType targetType,
			String targetValue, DateTime fromTimeStamp, DateTime toTimeStamp)
			throws InternetAccessException {

		if (fromTimeStamp == null || toTimeStamp == null) {
			throw new InternetAccessException(
					"FromDate and ToDate cannot be null");
		}

		if (fromTimeStamp.isAfter(toTimeStamp)) {
			throw new InternetAccessException(
					"FromDate cannot be greater than ToDate");
		}

		String strFromTimeStamp = fmtTimeStamp.print(fromTimeStamp);
		String strToTimeStamp = fmtTimeStamp.print(toTimeStamp);
		String strFromDate = fmtDay.print(fromTimeStamp);
		String strToDate = fmtDay.print(toTimeStamp);

		LOGGER.info("Will now fetch result from hive");
		List<InternetAccess> data;
		try {
			data = hiveDao.getIac(targetType, targetValue, strFromTimeStamp,
					strToTimeStamp, strFromDate, strToDate);
		} catch (HiveException e) {
			String msg = "Unable to fetch data from Hive";
			LOGGER.error(msg + " : " + e.getMessage());
			throw new InternetAccessException(msg, e);
		}

		List<String> rows = new ArrayList<String>();
		List<String> header = new ArrayList<String>();

		header.add("STOP_TIME");
		header.add("START_TIME");
		header.add("IP_ADDRESS");
		header.add("ACCESS_UNIT");
		header.add("HOSTNAME");
		header.add("INPUT_BYTES");
		header.add("OUTPUT_BYTES");
		header.add("CALLING_ST_ID");
		header.add("SESSION_ID");

		rows.add(StringUtils.collectionToCommaDelimitedString(header));

		for (InternetAccess item : data) {
			List<String> row = new ArrayList<String>();

			row.add(item.getStopTime());
			row.add(item.getStartTime());
			row.add(item.getClientIp());
			row.add(item.getAccessUnit());
			row.add(item.getHostname());
			row.add(item.getInputBytes());
			row.add(item.getOutputBytes());
			row.add(item.getCalledStId());
			row.add(item.getSessionId());

			rows.add(StringUtils.collectionToCommaDelimitedString(row));
		}

		String csvData = StringUtils.collectionToDelimitedString(rows, "\r\n");

		ByteArrayInputStream bais = new ByteArrayInputStream(csvData.getBytes());

		if (data.isEmpty()) {
			LOGGER.warn("Did not find any matching record from Hive");
			return null;
		}

		LOGGER.info("Found " + data.size() + " record(s) from Hive");
		return bais;

	}

	@Override
	public void loadData() throws InternetAccessException {

		File dir = new File(localDir);
		if (!dir.isDirectory()) {
			String msg = "Source path does not exist or is not a directory";
			LOGGER.error(msg);
			throw new InternetAccessException(msg);
		}

		// -------------------------------
		// LOADING LOCAL DATA TO HDFS
		// -------------------------------

		try {
			File remoteDir = new File(hdfsDir);
			String hdfsParentDir = remoteDir.getParent();
			LOGGER.info("Will now upload " + dir.listFiles().length
					+ " file(s) from directory LOCAL:" + localDir + " to HDFS:"
					+ hdfsDir);
			hdfsClient.deleteIfExist(hdfsDir);
			hdfsClient.copyFromLocal(localDir, hdfsParentDir);
		} catch (HDFSException e) {
			String msg = "Unable to load data to HDFS location " + hdfsDir;
			LOGGER.error(msg + " : " + e.getMessage());
			throw new InternetAccessException(msg, e);
		}

		// -------------------------------
		// LOADING HDFS DATA TO HIVE
		// -------------------------------

		try {
			LOGGER.info("Will now upload HDFS:" + hdfsDir
					+ " to Hive LIPS.INTERNET_ACCESS_LOAD table");
			hiveDao.loadIacFile(hdfsDir);
			LOGGER.info("Will now partition data into LIPS.INTERNET_ACCESS table");
			hiveDao.partitionIacData();
		} catch (HiveException e) {
			String msg = "Unable to Load data to Hive";
			LOGGER.error(msg + " : " + e.getMessage());
			throw new InternetAccessException(msg, e);
		}

	}

	@Autowired
	public void setHiveDao(HiveDao hiveDao) {
		this.hiveDao = hiveDao;
	}

	@Autowired
	public void setHdfsClient(HDFSService hdfsClient) {
		this.hdfsClient = hdfsClient;
	}

	@Value("${local.fs.dlf.dir}")
	public void setLocalDir(String localDir) {
		this.localDir = localDir;
	}

	@Value("${hadoop.hdfs.dlf.dir}")
	public void setHdfsDir(String hdfsDir) {
		this.hdfsDir = hdfsDir;
	}

}
