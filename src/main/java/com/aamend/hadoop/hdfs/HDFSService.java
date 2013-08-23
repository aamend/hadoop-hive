package com.aamend.hadoop.hdfs;

import java.io.File;
import java.util.List;

import org.apache.hadoop.conf.Configuration;


public interface HDFSService {

	// File Copy
	void copyFromLocal(String srcDir, String dstDir) throws HDFSException;
	void copyToLocal(String srcDir, String dstDir) throws HDFSException;

	// Stream directory
	List<File> readSequenceFilesFromDirectory(Configuration config,
                                              String srcDir, String dstDir, String filePattern)
			throws HDFSException;

	// File utils
	void rename(String fromthis, String tothis) throws HDFSException;
	void renameIfExist(String fromthis, String tothis) throws HDFSException;
	void delete(String srcFile) throws HDFSException;
	void deleteIfExist(String srcFile) throws HDFSException;
	void mkdir(String dstDir) throws HDFSException;
	void mkdirIfNotExist(String dstDir) throws HDFSException;

}
