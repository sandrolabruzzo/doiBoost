
package eu.dnetlib.doiboost.orcid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.orcid.Work;
import eu.dnetlib.dhp.schema.orcid.WorkDetail;
import eu.dnetlib.doiboost.orcid.util.HDFSUtil;
import eu.dnetlib.doiboost.orcidnodoi.xml.XMLRecordParserNoDoi;
import scala.Tuple2;

public class SparkUpdateOrcidWorks {

	public static final Logger logger = LoggerFactory.getLogger(SparkUpdateOrcidWorks.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.setSerializationInclusion(JsonInclude.Include.NON_NULL);

	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkUpdateOrcidWorks.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/download_orcid_data.json")));
		parser.parseArgument(args);
		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		final String workingPath = parser.get("workingPath");
		final String hdfsServerUri = parser.get("hdfsServerUri");

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

				LongAccumulator oldWorksFoundAcc = spark
					.sparkContext()
					.longAccumulator("old_works_found");
				LongAccumulator updatedWorksFoundAcc = spark
					.sparkContext()
					.longAccumulator("updated_works_found");
				LongAccumulator newWorksFoundAcc = spark
					.sparkContext()
					.longAccumulator("new_works_found");
				LongAccumulator errorCodeWorksFoundAcc = spark
					.sparkContext()
					.longAccumulator("error_code_works_found");
				LongAccumulator errorLoadingWorksJsonFoundAcc = spark
					.sparkContext()
					.longAccumulator("error_loading_works_json_found");
				LongAccumulator errorParsingWorksXMLFoundAcc = spark
					.sparkContext()
					.longAccumulator("error_parsing_works_xml_found");

				Function<String, Work> retrieveWorkFunction = jsonData -> {
					Work work = new Work();
					JsonElement jElement = new JsonParser().parse(jsonData);
					String statusCode = getJsonValue(jElement, "statusCode");
					work.setStatusCode(statusCode);
					work.setDownloadDate(Long.toString(System.currentTimeMillis()));
					if (statusCode.equals("200")) {
						String compressedData = getJsonValue(jElement, "compressedData");
						if (StringUtils.isEmpty(compressedData)) {
							errorLoadingWorksJsonFoundAcc.add(1);
						} else {
							String xmlWork = ArgumentApplicationParser.decompressValue(compressedData);
							try {
								WorkDetail workDetail = XMLRecordParserNoDoi
									.VTDParseWorkData(xmlWork.getBytes());
								work.setWorkDetail(workDetail);
								work.setBase64CompressData(compressedData);
								return work;
							} catch (Exception e) {
								logger.error("parsing xml [" + jsonData + "]", e);
								errorParsingWorksXMLFoundAcc.add(1);
							}
						}
					} else {
						errorCodeWorksFoundAcc.add(1);
					}
					return work;
				};

				Dataset<Work> downloadedWorksDS = spark
					.createDataset(
						sc
							.textFile(workingPath + "downloads/updated_works/*")
							.map(s -> s.substring(21, s.length() - 1))
							.map(retrieveWorkFunction)
							.rdd(),
						Encoders.bean(Work.class));
				Dataset<Work> currentWorksDS = spark
					.createDataset(
						sc
							.textFile(workingPath.concat("orcid_dataset/works/*"))
							.map(item -> OBJECT_MAPPER.readValue(item, Work.class))
							.rdd(),
						Encoders.bean(Work.class));
				currentWorksDS
					.joinWith(
						downloadedWorksDS,
						currentWorksDS
							.col("workDetail.id")
							.equalTo(downloadedWorksDS.col("workDetail.id"))
							.and(
								currentWorksDS
									.col("workDetail.oid")
									.equalTo(downloadedWorksDS.col("workDetail.oid"))),
						"full_outer")
					.map((MapFunction<Tuple2<Work, Work>, Work>) value -> {
						Optional<Work> opCurrent = Optional.ofNullable(value._1());
						Optional<Work> opDownloaded = Optional.ofNullable(value._2());
						if (!opCurrent.isPresent()) {
							newWorksFoundAcc.add(1);
							return opDownloaded.get();
						}
						if (!opDownloaded.isPresent()) {
							oldWorksFoundAcc.add(1);
							return opCurrent.get();
						}
						if (opCurrent.isPresent() && opDownloaded.isPresent()) {
							updatedWorksFoundAcc.add(1);
							return opDownloaded.get();
						}
						return null;
					},
						Encoders.bean(Work.class))
					.filter(Objects::nonNull)
					.toJavaRDD()
					.map(work -> OBJECT_MAPPER.writeValueAsString(work))
					.saveAsTextFile(workingPath.concat("orcid_dataset/new_works"), GzipCodec.class);

				logger.info("oldWorksFoundAcc: {}", oldWorksFoundAcc.value());
				logger.info("newWorksFoundAcc: {}", newWorksFoundAcc.value());
				logger.info("updatedWorksFoundAcc: {}", updatedWorksFoundAcc.value());
				logger.info("errorCodeWorksFoundAcc: {}", errorCodeWorksFoundAcc.value());
				logger.info("errorLoadingJsonWorksFoundAcc: {}", errorLoadingWorksJsonFoundAcc.value());
				logger.info("errorParsingXMLWorksFoundAcc: {}", errorParsingWorksXMLFoundAcc.value());

				String lastModifiedDateFromLambdaFile = HDFSUtil
					.readFromTextFile(hdfsServerUri, workingPath, "last_modified_date_from_lambda_file.txt");
				HDFSUtil.writeToTextFile(hdfsServerUri, workingPath, "last_update.txt", lastModifiedDateFromLambdaFile);
				logger.info("last_update file updated");
			});
	}

	private static String getJsonValue(JsonElement jElement, String property) {
		if (jElement.getAsJsonObject().has(property)) {
			JsonElement name = jElement.getAsJsonObject().get(property);
			if (name != null && !name.isJsonNull()) {
				return name.getAsString();
			}
		}
		return "";
	}
}
