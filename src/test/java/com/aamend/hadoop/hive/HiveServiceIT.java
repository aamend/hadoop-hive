package com.aamend.hadoop.hive;

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.util.Scanner;

import junit.framework.Assert;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration({ "/META-INF/*-context.xml" })
public class HiveServiceIT {

	@Autowired
	private InternetAccessService iacService;

	private String msisdn = "41794201636";
	private String ip = "10.17.114.72";
	private String strFromDate = "20130403000000";
	private String strToDate = "20130404000000";

	DateTimeFormatter fmtTimeStamp = DateTimeFormat
			.forPattern("yyyyMMddHHmmss");

	private static Logger LOGGER = LoggerFactory
			.getLogger(HiveServiceIT.class);

	private Scanner scanner;

	@Test
	public void searchByMsisdn() throws InternetAccessException,
			FileNotFoundException {

		DateTime fromDate = fmtTimeStamp.parseDateTime(strFromDate);
		DateTime toDate = fmtTimeStamp.parseDateTime(strToDate);

		ByteArrayInputStream bais = iacService.searchByMsisdn(msisdn, fromDate,
				toDate);
		Assert.assertNotNull(bais);
		scanner = new Scanner(bais);
		while (scanner.hasNext()) {
			LOGGER.debug(scanner.next());
		}

	}

	@Test
	public void searchByIp() throws InternetAccessException,
			FileNotFoundException {

		DateTime fromDate = fmtTimeStamp.parseDateTime(strFromDate);
		DateTime toDate = fmtTimeStamp.parseDateTime(strToDate);

		ByteArrayInputStream bais = iacService.searchByIp(ip, fromDate, toDate);
		Assert.assertNotNull(bais);
		scanner = new Scanner(bais);
		while (scanner.hasNext()) {
			LOGGER.debug(scanner.next());
		}

	}

	@Test
	public void loadData() throws InternetAccessException {
		iacService.loadData();
	}

}
