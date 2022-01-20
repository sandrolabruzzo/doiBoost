
package eu.dnetlib.doiboost.orcid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.common.collection.CollectorException;
import eu.dnetlib.dhp.common.collection.HttpClientParams;
import eu.dnetlib.doiboost.orcid.model.DownloadedRecordData;
import eu.dnetlib.doiboost.orcid.util.DownloadsReport;
import eu.dnetlib.doiboost.orcid.util.HDFSUtil;
import eu.dnetlib.doiboost.orcid.util.MultiAttemptsHttpConnector;
import scala.Tuple2;

public class SparkDownloadOrcidAuthors {

	static Logger logger = LoggerFactory.getLogger(SparkDownloadOrcidAuthors.class);
	static final String DATE_FORMAT = "yyyy-MM-dd HH:mm:ss";

	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkDownloadOrcidAuthors.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/download_orcid_data.json")));
		parser.parseArgument(args);
		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		logger.info("isSparkSessionManaged: {}", isSparkSessionManaged);
		final String workingPath = parser.get("workingPath");
		logger.info("workingPath: {}", workingPath);
		final String outputPath = parser.get("outputPath");
		logger.info("outputPath: {}", outputPath);
		final String token = parser.get("token");
		final String lambdaFileName = parser.get("lambdaFileName");
		logger.info("lambdaFileName: {}", lambdaFileName);
		final String hdfsServerUri = parser.get("hdfsServerUri");

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				String lastUpdate = HDFSUtil.readFromTextFile(hdfsServerUri, workingPath, "last_update.txt");
				logger.info("lastUpdate: {}", lastUpdate);
				if (StringUtils.isBlank(lastUpdate)) {
					throw new FileNotFoundException("last update info not found");
				}
				JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

				LongAccumulator parsedRecordsAcc = spark.sparkContext().longAccumulator("parsed_records");
				LongAccumulator modifiedRecordsAcc = spark.sparkContext().longAccumulator("to_download_records");
				LongAccumulator downloadedRecordsAcc = spark.sparkContext().longAccumulator("downloaded_records");
				LongAccumulator errorsAcc = spark.sparkContext().longAccumulator("errors");

				String lambdaFilePath = workingPath + lambdaFileName;
				logger.info("Retrieving data from lamda sequence file: " + lambdaFilePath);
				JavaPairRDD<Text, Text> lamdaFileRDD = sc
					.sequenceFile(lambdaFilePath, Text.class, Text.class);
				final long lamdaFileRDDCount = lamdaFileRDD.count();
				logger.info("Data retrieved: {}", lamdaFileRDDCount);

				Function<Tuple2<Text, Text>, Boolean> isModifiedAfterFilter = data -> {
					String orcidId = data._1().toString();
					String lastModifiedDate = data._2().toString();
					parsedRecordsAcc.add(1);
					if (isModified(orcidId, lastModifiedDate, lastUpdate)) {
						modifiedRecordsAcc.add(1);
						return true;
					}
					return false;
				};

				Function<Tuple2<Text, Text>, Tuple2<String, String>> downloadRecordFn = data -> {
					String orcidId = data._1().toString();
					String lastModifiedDate = data._2().toString();
					final DownloadedRecordData downloaded = new DownloadedRecordData();
					downloaded.setOrcidId(orcidId);
					downloaded.setLastModifiedDate(lastModifiedDate);
					final HttpClientParams clientParams = new HttpClientParams();
					MultiAttemptsHttpConnector httpConnector = new MultiAttemptsHttpConnector(clientParams);
					httpConnector.setAuthMethod(MultiAttemptsHttpConnector.BEARER);
					httpConnector.setAcceptHeaderValue("application/vnd.orcid+xml");
					httpConnector.setAuthToken(token);
					String apiUrl = "https://api.orcid.org/v3.0/" + orcidId + "/record";
					DownloadsReport report = new DownloadsReport();
					long startReq = System.currentTimeMillis();
					boolean downloadCompleted = false;
					String record = "";
					try {
						record = httpConnector.getInputSource(apiUrl, report);
						downloadCompleted = true;
					} catch (CollectorException ce) {
						if (!report.isEmpty()) {
							int errCode = report.keySet().stream().findFirst().get();
							report.forEach((k, v) -> {
								logger.error(k + " " + v);
							});
							downloaded.setStatusCode(errCode);
						} else {
							downloaded.setStatusCode(-4);
						}
						errorsAcc.add(1);
					}
					long endReq = System.currentTimeMillis();
					long reqTime = endReq - startReq;
					if (reqTime < 1000) {
						Thread.sleep(1000 - reqTime);
					}
					if (downloadCompleted) {
						downloaded.setStatusCode(200);
						downloadedRecordsAcc.add(1);
						downloaded
							.setCompressedData(
								ArgumentApplicationParser
									.compressArgument(record));
					}
					return downloaded.toTuple2();
				};

				sc.hadoopConfiguration().set("mapreduce.output.fileoutputformat.compress", "true");

				logger.info("Start execution ...");
				JavaPairRDD<Text, Text> authorsModifiedRDD = lamdaFileRDD.filter(isModifiedAfterFilter);
				long authorsModifiedCount = authorsModifiedRDD.count();
				logger.info("Authors modified count: {}", authorsModifiedCount);

				final JavaPairRDD<Text, Text> pairRDD = authorsModifiedRDD
					.repartition(100)
					.map(downloadRecordFn)
					.mapToPair(t -> new Tuple2<>(new Text(t._1()), new Text(t._2())));
				saveAsSequenceFile(workingPath, outputPath, sc, pairRDD);

				logger.info("parsedRecordsAcc: {}", parsedRecordsAcc.value());
				logger.info("modifiedRecordsAcc: {}", modifiedRecordsAcc.value());
				logger.info("downloadedRecordsAcc: {}", downloadedRecordsAcc.value());
				logger.info("errorsAcc: {}", errorsAcc.value());
			});
	}

	private static void saveAsSequenceFile(String workingPath, String outputPath, JavaSparkContext sc,
		JavaPairRDD<Text, Text> pairRDD) {
		pairRDD
			.saveAsNewAPIHadoopFile(
				workingPath.concat(outputPath),
				Text.class,
				Text.class,
				SequenceFileOutputFormat.class,
				sc.hadoopConfiguration());
	}

	public static boolean isModified(String orcidId, String modifiedDate, String lastUpdate) {
		Date modifiedDateDt;
		Date lastUpdateDt;
		String lastUpdateRedux = "";
		try {
			if (modifiedDate.equals("last_modified")) {
				return false;
			}
			if (modifiedDate.length() != 19) {
				modifiedDate = modifiedDate.substring(0, 19);
			}
			if (lastUpdate.length() != 19) {
				lastUpdateRedux = lastUpdate.substring(0, 19);
			} else {
				lastUpdateRedux = lastUpdate;
			}
			modifiedDateDt = new SimpleDateFormat(DATE_FORMAT).parse(modifiedDate);
			lastUpdateDt = new SimpleDateFormat(DATE_FORMAT).parse(lastUpdateRedux);
		} catch (Exception e) {
			throw new RuntimeException("[" + orcidId + "] modifiedDate <" + modifiedDate + "> lastUpdate <" + lastUpdate
				+ "> Parsing date: " + e.getMessage());
		}
		return modifiedDateDt.after(lastUpdateDt);
	}
}
