
package eu.dnetlib.doiboost.crossref;

import java.io.BufferedOutputStream;
import java.net.URI;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.mortbay.log.Log;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;

public class ExtractCrossrefRecords {
	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					ExtractCrossrefRecords.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/crossref_dump_reader/crossref_dump_reader.json")));
		parser.parseArgument(args);
		final String hdfsServerUri = parser.get("hdfsServerUri");
		final String workingPath = hdfsServerUri.concat(parser.get("workingPath"));
		final String outputPath = parser.get("outputPath");
		final String crossrefFileNameTarGz = parser.get("crossrefFileNameTarGz");

		Path hdfsreadpath = new Path(workingPath.concat("/").concat(crossrefFileNameTarGz));
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", workingPath);
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
		FileSystem fs = FileSystem.get(URI.create(workingPath), conf);
		FSDataInputStream crossrefFileStream = fs.open(hdfsreadpath);
		try (TarArchiveInputStream tais = new TarArchiveInputStream(
			new GzipCompressorInputStream(crossrefFileStream))) {
			TarArchiveEntry entry = null;
			while ((entry = tais.getNextTarEntry()) != null) {
				if (!entry.isDirectory()) {
					try (
						FSDataOutputStream out = fs
							.create(new Path(outputPath.concat(entry.getName()).concat(".gz")));
						GZIPOutputStream gzipOs = new GZIPOutputStream(new BufferedOutputStream(out))) {

						IOUtils.copy(tais, gzipOs);

					}

				}
			}
		}
		Log.info("Crossref dump reading completed");

	}
}
