
package eu.dnetlib.doiboost.orcid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;
import static org.apache.spark.sql.functions.*;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.orcid.AuthorSummary;
import eu.dnetlib.doiboost.orcid.xml.XMLRecordParser;
import scala.Tuple2;

public class SparkUpdateOrcidAuthors {

	public static final Logger logger = LoggerFactory.getLogger(SparkUpdateOrcidAuthors.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper()
		.setSerializationInclusion(JsonInclude.Include.NON_NULL);

	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkUpdateOrcidAuthors.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/download_orcid_data.json")));
		parser.parseArgument(args);
		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		final String workingPath = parser.get("workingPath");

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

				LongAccumulator oldAuthorsFoundAcc = spark
					.sparkContext()
					.longAccumulator("old_authors_found");
				LongAccumulator updatedAuthorsFoundAcc = spark
					.sparkContext()
					.longAccumulator("updated_authors_found");
				LongAccumulator newAuthorsFoundAcc = spark
					.sparkContext()
					.longAccumulator("new_authors_found");
				LongAccumulator errorCodeAuthorsFoundAcc = spark
					.sparkContext()
					.longAccumulator("error_code_authors_found");
				LongAccumulator errorLoadingAuthorsJsonFoundAcc = spark
					.sparkContext()
					.longAccumulator("error_loading_authors_json_found");
				LongAccumulator errorParsingAuthorsXMLFoundAcc = spark
					.sparkContext()
					.longAccumulator("error_parsing_authors_xml_found");

				Function<Tuple2<Text, Text>, AuthorSummary> retrieveAuthorSummaryFunction = data -> {
					AuthorSummary authorSummary = new AuthorSummary();
					String orcidId = data._1().toString();
					String jsonData = data._2().toString();
					JsonElement jElement = new JsonParser().parse(jsonData);
					String statusCode = getJsonValue(jElement, "statusCode");

					if (statusCode.equals("200")) {
						String compressedData = getJsonValue(jElement, "compressedData");
						if (StringUtils.isEmpty(compressedData)) {
							errorLoadingAuthorsJsonFoundAcc.add(1);
						} else {
							String xmlAuthor = ArgumentApplicationParser.decompressValue(compressedData);
							try {
								authorSummary = XMLRecordParser
									.VTDParseAuthorSummary(xmlAuthor.getBytes());
								authorSummary.setStatusCode(statusCode);
								authorSummary.setDownloadDate(Long.toString(System.currentTimeMillis()));
								authorSummary.setBase64CompressData(compressedData);
								return authorSummary;
							} catch (Exception e) {
								logger.error("parsing xml " + orcidId + " [" + jsonData + "]", e);
								errorParsingAuthorsXMLFoundAcc.add(1);
							}
						}
					} else {
						authorSummary.setStatusCode(statusCode);
						authorSummary.setDownloadDate(Long.toString(System.currentTimeMillis()));
						errorCodeAuthorsFoundAcc.add(1);
					}
					return authorSummary;
				};

				Dataset<AuthorSummary> downloadedAuthorSummaryDS = spark
					.createDataset(
						sc
							.sequenceFile(workingPath + "downloads/updated_authors/*", Text.class, Text.class)
							.map(retrieveAuthorSummaryFunction)
							.rdd(),
						Encoders.bean(AuthorSummary.class));
				Dataset<AuthorSummary> currentAuthorSummaryDS = spark
					.createDataset(
						sc
							.textFile(workingPath.concat("orcid_dataset/authors/*"))
							.map(item -> OBJECT_MAPPER.readValue(item, AuthorSummary.class))
							.rdd(),
						Encoders.bean(AuthorSummary.class));
				Dataset<AuthorSummary> mergedAuthorSummaryDS = currentAuthorSummaryDS
					.joinWith(
						downloadedAuthorSummaryDS,
						currentAuthorSummaryDS
							.col("authorData.oid")
							.equalTo(downloadedAuthorSummaryDS.col("authorData.oid")),
						"full_outer")
					.map((MapFunction<Tuple2<AuthorSummary, AuthorSummary>, AuthorSummary>) value -> {
						Optional<AuthorSummary> opCurrent = Optional.ofNullable(value._1());
						Optional<AuthorSummary> opDownloaded = Optional.ofNullable(value._2());
						if (!opCurrent.isPresent()) {
							newAuthorsFoundAcc.add(1);
							return opDownloaded.get();
						}
						if (!opDownloaded.isPresent()) {
							oldAuthorsFoundAcc.add(1);
							return opCurrent.get();
						}
						if (opCurrent.isPresent() && opDownloaded.isPresent()) {
							updatedAuthorsFoundAcc.add(1);
							return opDownloaded.get();
						}
						return null;
					},
						Encoders.bean(AuthorSummary.class))
					.filter(Objects::nonNull);

				long mergedCount = mergedAuthorSummaryDS.count();

				Dataset<AuthorSummary> base64DedupedDS = mergedAuthorSummaryDS.dropDuplicates("base64CompressData");

				List<String> dupOids = base64DedupedDS
					.groupBy("authorData.oid")
					.agg(count("authorData.oid").alias("oidOccurrenceCount"))
					.where("oidOccurrenceCount > 1")
					.select("oid")
					.toJavaRDD()
					.map(row -> row.get(0).toString())
					.collect();

				JavaRDD<AuthorSummary> dupAuthors = base64DedupedDS
					.toJavaRDD()
					.filter(
						authorSummary -> (Objects.nonNull(authorSummary.getAuthorData())
							&& Objects.nonNull(authorSummary.getAuthorData().getOid())))
					.filter(authorSummary -> dupOids.contains(authorSummary.getAuthorData().getOid()));

				Dataset<AuthorSummary> dupAuthorSummaryDS = spark
					.createDataset(
						dupAuthors.rdd(),
						Encoders.bean(AuthorSummary.class));
				List<Tuple2<String, String>> lastModifiedAuthors = dupAuthorSummaryDS
					.groupBy("authorData.oid")
					.agg(array_max(collect_list("downloadDate")))
					.map(
						(MapFunction<Row, Tuple2<String, String>>) row -> new Tuple2<>(row.get(0).toString(),
							row.get(1).toString()),
						Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
					.toJavaRDD()
					.collect();

				JavaRDD<AuthorSummary> lastDownloadedAuthors = base64DedupedDS
					.toJavaRDD()
					.filter(
						authorSummary -> (Objects.nonNull(authorSummary.getAuthorData())
							&& Objects.nonNull(authorSummary.getAuthorData().getOid())))
					.filter(authorSummary -> {
						boolean oidFound = lastModifiedAuthors
							.stream()
							.filter(a -> a._1().equals(authorSummary.getAuthorData().getOid()))
							.count() == 1;
						boolean tsFound = lastModifiedAuthors
							.stream()
							.filter(
								a -> a._1().equals(authorSummary.getAuthorData().getOid()) &&
									a._2().equals(authorSummary.getDownloadDate()))
							.count() == 1;
						return !oidFound || tsFound;
					});

				Dataset<AuthorSummary> cleanedDS = spark
					.createDataset(
						lastDownloadedAuthors.rdd(),
						Encoders.bean(AuthorSummary.class))
					.dropDuplicates("downloadDate", "authorData");
				cleanedDS
					.toJavaRDD()
					.map(OBJECT_MAPPER::writeValueAsString)
					.saveAsTextFile(workingPath.concat("orcid_dataset/new_authors"), GzipCodec.class);
				long cleanedDSCount = cleanedDS.count();

				logger.info("report_oldAuthorsFoundAcc: {}", oldAuthorsFoundAcc.value());
				logger.info("report_newAuthorsFoundAcc: {}", newAuthorsFoundAcc.value());
				logger.info("report_updatedAuthorsFoundAcc: {}", updatedAuthorsFoundAcc.value());
				logger.info("report_errorCodeFoundAcc: {}", errorCodeAuthorsFoundAcc.value());
				logger.info("report_errorLoadingJsonFoundAcc: {}", errorLoadingAuthorsJsonFoundAcc.value());
				logger.info("report_errorParsingXMLFoundAcc: {}", errorParsingAuthorsXMLFoundAcc.value());
				logger.info("report_merged_count: {}", mergedCount);
				logger.info("report_cleaned_count: {}", cleanedDSCount);
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
