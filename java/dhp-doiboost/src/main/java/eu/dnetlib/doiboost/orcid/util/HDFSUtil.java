
package eu.dnetlib.doiboost.orcid.util;

import java.io.*;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HDFSUtil {

	static Logger logger = LoggerFactory.getLogger(HDFSUtil.class);

	private HDFSUtil() {
	}

	private static FileSystem getFileSystem(String hdfsServerUri) throws IOException {
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", hdfsServerUri);
		return FileSystem.get(conf);
	}

	public static String readFromTextFile(String hdfsServerUri, String workingPath, String path) throws IOException {
		FileSystem fileSystem = getFileSystem(hdfsServerUri);
		Path toReadPath = new Path(workingPath.concat(path));
		if (!fileSystem.exists(toReadPath)) {
			throw new IOException("File not exist: " + path);
		}
		logger.info("Last_update_path {}", toReadPath);
		FSDataInputStream inputStream = new FSDataInputStream(fileSystem.open(toReadPath));
		try (BufferedReader br = new BufferedReader(new InputStreamReader(inputStream))) {
			StringBuilder sb = new StringBuilder();

			String line;
			while ((line = br.readLine()) != null) {
				sb.append(line);
			}

			String buffer = sb.toString();
			logger.info("Last_update: {}", buffer);
			return buffer;
		}
	}

	public static void writeToTextFile(String hdfsServerUri, String workingPath, String path, String text)
		throws IOException {
		FileSystem fileSystem = getFileSystem(hdfsServerUri);
		Path toWritePath = new Path(workingPath.concat(path));
		if (fileSystem.exists(toWritePath)) {
			fileSystem.delete(toWritePath, true);
		}
		FSDataOutputStream os = fileSystem.create(toWritePath);
		try (BufferedWriter br = new BufferedWriter(new OutputStreamWriter(os, StandardCharsets.UTF_8))) {
			br.write(text);
		}
	}
}
