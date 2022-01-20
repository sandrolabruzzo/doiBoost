
package eu.dnetlib.doiboost.orcid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.mortbay.log.Log;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.doiboost.orcid.util.HDFSUtil;

public class SparkGenLastModifiedSeq {

	public static void main(String[] args) throws Exception {
		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkGenLastModifiedSeq.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/download_orcid_data.json")));
		parser.parseArgument(args);
		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);

		final String hdfsServerUri = parser.get("hdfsServerUri");
		final String workingPath = parser.get("workingPath");
		final String outputPath = parser.get("outputPath");
		final String lambdaFileName = parser.get("lambdaFileName");

		final String lambdaFileUri = hdfsServerUri.concat(workingPath).concat(lambdaFileName);
		final String lastModifiedDateFromLambdaFileUri = "last_modified_date_from_lambda_file.txt";

		SparkConf sparkConf = new SparkConf();
		runWithSparkSession(
			sparkConf,
			isSparkSessionManaged,
			spark -> {
				int rowsNum = 0;
				String lastModifiedAuthorDate = "";
				Path output = new Path(
					hdfsServerUri
						.concat(workingPath)
						.concat(outputPath));
				Path hdfsreadpath = new Path(lambdaFileUri);
				Configuration conf = spark.sparkContext().hadoopConfiguration();
				conf.set("fs.defaultFS", hdfsServerUri.concat(workingPath));
				conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
				conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
				FileSystem fs = FileSystem.get(URI.create(hdfsServerUri.concat(workingPath)), conf);
				FSDataInputStream lambdaFileStream = fs.open(hdfsreadpath);
				try (TarArchiveInputStream tais = new TarArchiveInputStream(
					new GzipCompressorInputStream(lambdaFileStream))) {
					TarArchiveEntry entry = null;
					try (SequenceFile.Writer writer = SequenceFile
						.createWriter(
							conf,
							SequenceFile.Writer.file(output),
							SequenceFile.Writer.keyClass(Text.class),
							SequenceFile.Writer.valueClass(Text.class),
							SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new GzipCodec()))) {
						while ((entry = tais.getNextTarEntry()) != null) {
							BufferedReader br = new BufferedReader(new InputStreamReader(tais));
							String line;
							while ((line = br.readLine()) != null) {
								String[] values = line.split(",");
								List<String> recordInfo = Arrays.asList(values);
								String orcidId = recordInfo.get(0);
								final Text key = new Text(orcidId);
								final Text value = new Text(recordInfo.get(3));
								writer.append(key, value);
								rowsNum++;
								if (rowsNum == 2) {
									lastModifiedAuthorDate = value.toString();
								}
							}

						}
					}
				}
				HDFSUtil
					.writeToTextFile(
						hdfsServerUri, workingPath, lastModifiedDateFromLambdaFileUri, lastModifiedAuthorDate);
				Log.info("Saved rows from lamda csv tar file: " + rowsNum);
			});
	}
}
