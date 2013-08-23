package com.aamend.hadoop.hive;

import java.util.List;

public interface HiveDao {

	List<InternetAccess> getIac(InternetAccessService.TargetType targetType,
                                String targetValue, String strFromTimeStamp, String strToTimeStamp,
                                String strFromDate, String strToDate) throws HiveException;

	void loadIacFile(String path) throws HiveException;

	void partitionIacData() throws HiveException;

}
