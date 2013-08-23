package com.aamend.hadoop.hdfs;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader.Option;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;


@Service
public class HDFSServiceBean implements HDFSService {

	private String hdfsUri;
	private String hdfsUser;

	private static Logger LOGGER = LoggerFactory
			.getLogger(HDFSServiceBean.class);

	@Override
	public void copyFromLocal(String source, String dest) throws HDFSException {

		FileSystem hdfs = mountHDFS();
		LOGGER.debug("Copy " + source + " into " + dest);
		Path srcPath = new Path(source);
		Path dstPath = new Path(dest);

		// Check if the remote file exists
		if (!isExist(hdfs, dest)) {
			String msg = "No such destination " + dstPath;
			dieAndUnmountHdfs(msg, hdfs);
		}

		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1,
				source.length());

		try {
			hdfs.copyFromLocalFile(srcPath, dstPath);
			LOGGER.info("File " + filename + "copied to " + dest);
		} catch (IOException e) {
			LOGGER.error("Exception caught! :" + e.getMessage());
			throw new HDFSException("Exception caught! :" + e.getMessage());
		} finally {
			unMount(hdfs);
		}

	}

	@Override
	public void copyToLocal(String source, String dest) throws HDFSException {

		FileSystem hdfs = mountHDFS();
		LOGGER.debug("Copy " + source + " into " + dest);
		Path srcPath = new Path(source);
		Path dstPath = new Path(dest);

		// Check if the remote file exists
		if (!isExist(hdfs, source)) {
			String msg = "No such source " + srcPath;
			dieAndUnmountHdfs(msg, hdfs);
		}

		// Get the filename out of the file path
		String filename = source.substring(source.lastIndexOf('/') + 1,
				source.length());

		try {
			hdfs.copyToLocalFile(srcPath, dstPath);
			LOGGER.info("File " + filename + "copied to " + dest);
		} catch (IOException e) {
			LOGGER.error("Exception caught! :" + e.getMessage());
			throw new HDFSException("Exception caught! :" + e.getMessage());
		} finally {
			unMount(hdfs);
		}

	}

	@Override
	public void rename(String fromthis, String tothis) throws HDFSException {

		FileSystem hdfs = mountHDFS();
		LOGGER.debug("Renaming file " + fromthis + " into " + tothis);
		Path fromPath = new Path(fromthis);
		Path toPath = new Path(tothis);

		// Check if the file exists
		if (!isExist(hdfs, fromthis)) {
			String msg = "No such destination " + fromPath;
			dieAndUnmountHdfs(msg, hdfs);
		}

		// Check if the file exists
		if (isExist(hdfs, tothis)) {
			String msg = "File " + toPath + " already exist";
			dieAndUnmountHdfs(msg, hdfs);
		}

		try {
			hdfs.rename(fromPath, toPath);
			LOGGER.info("Renamed from " + fromthis + "to " + tothis);
		} catch (IOException e) {
			LOGGER.error("Exception caught! :" + e.getMessage());
			throw new HDFSException("Exception caught! :" + e.getMessage());
		} finally {
			unMount(hdfs);
		}

	}

	@Override
	public void renameIfExist(String fromthis, String tothis)
			throws HDFSException {

		FileSystem hdfs = mountHDFS();
		LOGGER.debug("Renaming file " + fromthis + " into " + tothis);
		Path fromPath = new Path(fromthis);
		Path toPath = new Path(tothis);

		// Check if the file exists
		if (isExist(hdfs, fromthis)) {

			// Check if the file exists
			if (isExist(hdfs, tothis)) {
				String msg = "File " + toPath + " already exist";
				dieAndUnmountHdfs(msg, hdfs);
			}

			try {
				hdfs.rename(fromPath, toPath);
				LOGGER.info("Renamed from " + fromthis + "to " + tothis);
			} catch (IOException e) {
				LOGGER.error("Exception caught! :" + e.getMessage());
				throw new HDFSException("Exception caught! :" + e.getMessage());
			} finally {
				unMount(hdfs);
			}
		} else {
			String msg = "No such destination " + fromPath;
			LOGGER.warn(msg);
		}

	}

	@Override
	public List<File> readSequenceFilesFromDirectory(Configuration config,
			String srcDir, String dstDir, String filePattern)
			throws HDFSException {

		FileSystem hdfs = mountHDFS();
		LOGGER.debug("Reading sequences files from HDFS:" + srcDir
				+ " and download to LOCAL:" + dstDir);
		List<File> list = new ArrayList<File>();

		// Check if the file exist
		if (!isExist(hdfs, srcDir)) {
			String msg = "No such destination " + srcDir;
			dieAndUnmountHdfs(msg, hdfs);
		}

		// Check if the source is a directory
		if (!isDirectory(hdfs, srcDir)) {
			String msg = srcDir + " is not a directory";
			dieAndUnmountHdfs(msg, hdfs);
		}

		// Read files
		FileStatus[] fss;
		try {
			fss = hdfs.globStatus(new Path(srcDir + "/" + filePattern + "*"));
		} catch (IOException e) {
			String msg = "Cannot get file status from directory " + srcDir;
			LOGGER.error(msg);
			throw new HDFSException(msg + " " + e.getMessage());
		} finally {
			unMount(hdfs);
		}

		// Make sure hadoop directory in tomcat work folder does exist
		// If not, re-create it
		File dir = new File(dstDir + "/");
		boolean exists = dir.exists();
		if (!exists) {
			if (dir.mkdir()) {
				LOGGER.debug("Directory " + dstDir + " was not found but "
						+ "has been created");
			} else {
				String msg = "Directory " + dstDir
						+ " does not exist and cannot be created";
				dieAndUnmountHdfs(msg, hdfs);
			}
		}

		// For each file in this directory (recursive)
		for (FileStatus status : fss) {

			Path tmpPath = status.getPath();
			String fileName = tmpPath.getName();

			File file = new File(dstDir + "/" + fileName);

			FileOutputStream baos = null;

			LOGGER.debug("Will now download file " + fileName);
			SequenceFile.Reader reader = null;
			try {

				baos = new FileOutputStream(file);
				Option pathOpt = SequenceFile.Reader.file(tmpPath);
				reader = new SequenceFile.Reader(config, pathOpt);

				// Configure Sequence file
				Writable key = (Writable) ReflectionUtils.newInstance(
						reader.getKeyClass(), config);
				Writable value = (Writable) ReflectionUtils.newInstance(
						reader.getValueClass(), config);

				// Read Sequence file and write each line to Output Stream
				reader.getPosition();
				while (reader.next(key, value)) {
					baos.write(key.toString().getBytes());
					baos.write(',');
					baos.write(value.toString().getBytes());
					baos.write('\n');
					reader.getPosition();
				}
				baos.flush();
				reader.close();
				list.add(file);

			} catch (IOException e) {
				String msg = "There was a problem while downloading file "
						+ fileName + " from directory " + srcDir;
				LOGGER.error(msg + " " + e.getMessage());
				throw new HDFSException(msg + " " + e.getMessage());
			} finally {
				if (baos != null) {
					try {
						baos.close();
					} catch (IOException e) {
						String msg = "Cannot close output stream";
						LOGGER.error(msg + " " + e.getMessage());
						throw new HDFSException(msg + " " + e.getMessage());
					} finally {
						unMount(hdfs);
					}
				}
				unMount(hdfs);
			}
		}
		return list;
	}

	@Override
	public void delete(String source) throws HDFSException {

		FileSystem hdfs = mountHDFS();
		LOGGER.debug("Deleting " + source + " recursively");
		Path path = new Path(source);
		// Check if the file exist
		if (!isExist(hdfs, source)) {
			String msg = "No such destination " + source;
			dieAndUnmountHdfs(msg, hdfs);
		}

		// Delete the file
		try {
			hdfs.delete(path, true);
		} catch (IOException e) {
			String msg = "There was a problem while deleting "
					+ "files from path " + path;
			LOGGER.error(msg);
			throw new HDFSException(msg + " " + e.getMessage());
		} finally {
			unMount(hdfs);
		}

	}

	@Override
	public void deleteIfExist(String source) throws HDFSException {

		FileSystem hdfs = mountHDFS();
		LOGGER.debug("Deleting " + source + " recursively");
		Path path = new Path(source);
		// Check if the file exist
		if (isExist(hdfs, source)) {

			// Delete the file
			try {
				hdfs.delete(path, true);
			} catch (IOException e) {
				String msg = "There was a problem while deleting "
						+ "files from path " + path;
				LOGGER.error(msg);
				throw new HDFSException(msg + " " + e.getMessage());
			} finally {
				unMount(hdfs);
			}

		} else {
			String msg = "No such destination " + source;
			LOGGER.warn(msg);
		}

	}

	@Override
	public void mkdir(String dir) throws HDFSException {

		FileSystem hdfs = mountHDFS();
		LOGGER.debug("Creating directory " + dir);
		Path path = new Path(dir);

		// Check if the file exist
		if (isExist(hdfs, dir)) {
			String msg = dir + " already exist";
			dieAndUnmountHdfs(msg, hdfs);
		}

		// mkdir
		try {
			hdfs.mkdirs(path);
		} catch (IOException e) {
			String msg = "There was a problem while creating directory " + path;
			LOGGER.error(msg);
			throw new HDFSException(msg + " " + e.getMessage());
		} finally {
			unMount(hdfs);
		}

	}

	@Override
	public void mkdirIfNotExist(String dir) throws HDFSException {

		FileSystem hdfs = mountHDFS();
		LOGGER.debug("Creating directory " + dir);
		Path path = new Path(dir);

		// Check if the file exist
		if (!isExist(hdfs, dir)) {

			// mkdir
			try {
				hdfs.mkdirs(path);
			} catch (IOException e) {
				String msg = "There was a problem while creating directory "
						+ path;
				LOGGER.error(msg);
				throw new HDFSException(msg + " " + e.getMessage());
			} finally {
				unMount(hdfs);
			}

		} else {
			String msg = dir + " already exist";
			LOGGER.warn(msg);
		}
	}

	// Check whether a given path exist on HDFS
	private boolean isExist(FileSystem hdfs, String source)
			throws HDFSException {

		Path srcPath = new Path(source);
		// Check if the file exists
		try {
			if (hdfs.exists(srcPath)) {
				return true;
			} else {
				return false;
			}
		} catch (IOException e) {
			String msg = "Cannot check whether file exist on HDFS";
			LOGGER.error(msg);
			throw new HDFSException(msg + " " + e.getMessage());
		}

	}

	// Check whether a given path is file or directory
	private boolean isDirectory(FileSystem hdfs, String source)
			throws HDFSException {

		Path srcPath = new Path(source);
		// Check if the file exists
		try {
			if (hdfs.isDirectory(srcPath)) {
				return true;
			} else {
				return false;
			}
		} catch (IOException e) {
			String msg = "Cannot check whether file is a directory";
			LOGGER.error(msg);
			throw new HDFSException(msg + " " + e.getMessage());
		}

	}

	private Configuration createConf() {
		LOGGER.debug("Creating a new Configuration for HDFS access");
		Configuration config = new Configuration();
		System.setProperty("HADOOP_USER_NAME", hdfsUser);
		config.set("fs.defaultFS", hdfsUri);
		return config;
	}

	private FileSystem mountHDFS() throws HDFSException {

		try {
			Configuration config = createConf();
			LOGGER.debug("Mounting HDFS");
			return FileSystem.get(config);
		} catch (IOException e) {
			String msg = "cannot mount HDFS";
			LOGGER.error(msg);
			throw new HDFSException(msg + " " + e.getMessage());
		}

	}

	private void unMount(FileSystem hdfs) throws HDFSException {

		try {
			LOGGER.debug("Unmounting HDFS");
			hdfs.close();
		} catch (IOException e) {
			String msg = "cannot close HDFS";
			LOGGER.error(msg);
			throw new HDFSException(msg + " " + e.getMessage());
		}
	}

	private void dieAndUnmountHdfs(String msg, FileSystem hdfs)
			throws HDFSException {
		LOGGER.error(msg);
		unMount(hdfs);
		throw new HDFSException(msg);
	}

	@Value("${hadoop.hdfs.uri}")
	public void setHdfsUri(String hdfsUri) {
		this.hdfsUri = hdfsUri;
	}

	@Value("${hadoop.hdfs.user}")
	public void setHdfsUser(String hdfsUser) {
		this.hdfsUser = hdfsUser;
	}

}
