
package eu.dnetlib.doiboost.orcid;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.minlog.Log;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.orcid.AuthorData;
import eu.dnetlib.dhp.schema.orcid.OrcidDOI;
import eu.dnetlib.doiboost.orcid.model.WorkData;
import eu.dnetlib.doiboost.orcid.xml.XMLRecordParser;
import eu.dnetlib.doiboost.orcidnodoi.json.JsonWriter;
import scala.Tuple2;

public class SparkGenerateDoiAuthorList {

	public static void main(String[] args) throws Exception {
		Logger logger = LoggerFactory.getLogger(SparkGenerateDoiAuthorList.class);
		logger.info("[ SparkGenerateDoiAuthorList STARTED]");

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkGenerateDoiAuthorList.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/gen_doi_author_list_orcid_parameters.json")));
		parser.parseArgument(args);
		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		logger.info("isSparkSessionManaged: {}", isSparkSessionManaged);
		final String workingPath = parser.get("workingPath");
		logger.info("workingPath: {}", workingPath);
		final String outputDoiAuthorListPath = parser.get("outputDoiAuthorListPath");
		logger.info("outputDoiAuthorListPath: {}", outputDoiAuthorListPath);
		final String authorsPath = parser.get("authorsPath");
		logger.info("authorsPath: {}", authorsPath);
		final String xmlWorksPath = parser.get("xmlWorksPath");
		logger.info("xmlWorksPath: {}", xmlWorksPath);

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

				JavaPairRDD<Text, Text> summariesRDD = sc
					.sequenceFile(workingPath.concat(authorsPath), Text.class, Text.class);
				Dataset<AuthorData> summariesDataset = spark
					.createDataset(
						summariesRDD.map(seq -> loadAuthorFromJson(seq._1(), seq._2())).rdd(),
						Encoders.bean(AuthorData.class));

				JavaPairRDD<Text, Text> xmlWorksRDD = sc
					.sequenceFile(workingPath.concat(xmlWorksPath), Text.class, Text.class);

				Dataset<WorkData> activitiesDataset = spark
					.createDataset(
						xmlWorksRDD
							.map(seq -> XMLRecordParser.VTDParseWorkData(seq._2().toString().getBytes()))
							.filter(work -> work != null && work.getErrorCode() == null && work.isDoiFound())
							.rdd(),
						Encoders.bean(WorkData.class));

				Function<Tuple2<String, AuthorData>, Tuple2<String, List<AuthorData>>> toAuthorListFunction = data -> {
					try {
						String doi = data._1();
						if (doi == null) {
							return null;
						}
						AuthorData author = data._2();
						if (author == null) {
							return null;
						}
						List<AuthorData> toAuthorList = Arrays.asList(author);
						return new Tuple2<>(doi, toAuthorList);
					} catch (Exception e) {
						Log.error("toAuthorListFunction ERROR", e);
						return null;
					}
				};

				JavaRDD<Tuple2<String, List<AuthorData>>> doisRDD = activitiesDataset
					.joinWith(
						summariesDataset,
						activitiesDataset.col("oid").equalTo(summariesDataset.col("oid")), "inner")
					.map(
						(MapFunction<Tuple2<WorkData, AuthorData>, Tuple2<String, AuthorData>>) value -> {
							WorkData w = value._1;
							AuthorData a = value._2;
							return new Tuple2<>(w.getDoi(), a);
						},
						Encoders.tuple(Encoders.STRING(), Encoders.bean(AuthorData.class)))
					.filter(Objects::nonNull)
					.toJavaRDD()
					.map(toAuthorListFunction);

				JavaPairRDD
					.fromJavaRDD(doisRDD)
					.reduceByKey((d1, d2) -> {
						try {
							if (d1 != null && d2 != null) {
								Stream<AuthorData> mergedStream = Stream
									.concat(
										d1.stream(),
										d2.stream());
								return mergedStream.collect(Collectors.toList());
							}
							if (d1 != null) {
								return d1;
							}
							if (d2 != null) {
								return d2;
							}
						} catch (Exception e) {
							Log.error("mergeAuthorsFunction ERROR", e);
							return null;
						}
						return null;
					})
					.mapToPair(s -> {
						List<AuthorData> authorList = s._2();
						Set<String> oidsAlreadySeen = new HashSet<>();
						authorList.removeIf(a -> !oidsAlreadySeen.add(a.getOid()));
						return new Tuple2<>(s._1(), authorList);
					})
					.map(s -> {
						OrcidDOI orcidDOI = new OrcidDOI();
						orcidDOI.setDoi(s._1());
						orcidDOI.setAuthors(s._2());
						return JsonWriter.create(orcidDOI);
					})
					.saveAsTextFile(workingPath + outputDoiAuthorListPath, GzipCodec.class);
			});

	}

	private static AuthorData loadAuthorFromJson(Text orcidId, Text json) {
		AuthorData authorData = new AuthorData();
		authorData.setOid(orcidId.toString());
		JsonElement jElement = new JsonParser().parse(json.toString());
		authorData.setName(getJsonValue(jElement, "name"));
		authorData.setSurname(getJsonValue(jElement, "surname"));
		authorData.setCreditName(getJsonValue(jElement, "creditname"));
		return authorData;
	}

	private static String getJsonValue(JsonElement jElement, String property) {
		if (jElement.getAsJsonObject().has(property)) {
			JsonElement name = null;
			name = jElement.getAsJsonObject().get(property);
			if (name != null && !name.isJsonNull()) {
				return name.getAsString();
			}
		}
		return null;
	}
}
