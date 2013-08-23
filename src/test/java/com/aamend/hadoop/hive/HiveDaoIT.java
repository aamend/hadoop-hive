package com.aamend.hadoop.hive;

import java.sql.SQLException;
import java.util.List;

import junit.framework.Assert;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/META-INF/hadoop-context.xml" })
public class HiveDaoIT {

	@Autowired
	private HiveDao hiveDao;

	private String ip = "10.144.118.2";
	private String strFromTimestamp = "20120930000000";
	private String strToTimestamp = "20120930000000";

	DateTimeFormatter fmtTimeStamp = DateTimeFormat
			.forPattern("yyyyMMddHHmmss");
	DateTimeFormatter fmtDay = DateTimeFormat.forPattern("yyyyMMdd");

	@Test
	public void fetchData() throws HiveException, SQLException {

		DateTime fromDate = fmtTimeStamp.parseDateTime(strFromTimestamp);
		DateTime toDate = fmtTimeStamp.parseDateTime(strToTimestamp);

		String strFromDate = fmtDay.print(fromDate);
		String strToDate = fmtDay.print(toDate);

		List<InternetAccess> iacs = hiveDao.getIac(InternetAccessService.TargetType.IP, ip,
				strFromTimestamp, strToTimestamp, strFromDate, strToDate);
		Assert.assertNotNull(iacs);
	}

}
