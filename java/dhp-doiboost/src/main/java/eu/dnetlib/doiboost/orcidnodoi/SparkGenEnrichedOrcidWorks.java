
package eu.dnetlib.doiboost.orcidnodoi;

import static eu.dnetlib.dhp.common.SparkSessionSupport.runWithSparkSession;

import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.dnetlib.dhp.application.ArgumentApplicationParser;
import eu.dnetlib.dhp.schema.action.AtomicAction;
import eu.dnetlib.dhp.schema.oaf.Publication;
import eu.dnetlib.dhp.schema.orcid.*;
import eu.dnetlib.doiboost.orcid.json.JsonHelper;
import eu.dnetlib.doiboost.orcid.util.HDFSUtil;
import eu.dnetlib.doiboost.orcidnodoi.oaf.PublicationToOaf;
import eu.dnetlib.doiboost.orcidnodoi.similarity.AuthorMatcher;
import scala.Tuple2;

/**
 * This spark job generates orcid publications no doi dataset
 */

public class SparkGenEnrichedOrcidWorks {

	static Logger logger = LoggerFactory.getLogger(SparkGenEnrichedOrcidWorks.class);

	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

	public static void main(String[] args) throws Exception {

		final ArgumentApplicationParser parser = new ArgumentApplicationParser(
			IOUtils
				.toString(
					SparkGenEnrichedOrcidWorks.class
						.getResourceAsStream(
							"/eu/dnetlib/dhp/doiboost/gen_orcid-no-doi_params.json")));
		parser.parseArgument(args);
		Boolean isSparkSessionManaged = Optional
			.ofNullable(parser.get("isSparkSessionManaged"))
			.map(Boolean::valueOf)
			.orElse(Boolean.TRUE);
		final String hdfsServerUri = parser.get("hdfsServerUri");
		final String workingPath = parser.get("workingPath");
		final String outputEnrichedWorksPath = parser.get("outputEnrichedWorksPath");
		final String orcidDataFolder = parser.get("orcidDataFolder");

		SparkConf conf = new SparkConf();
		runWithSparkSession(
			conf,
			isSparkSessionManaged,
			spark -> {
				String lastUpdate = HDFSUtil.readFromTextFile(hdfsServerUri, workingPath, "last_update.txt");
				if (StringUtils.isBlank(lastUpdate)) {
					throw new FileNotFoundException("last update info not found");
				}
				final String dateOfCollection = lastUpdate.substring(0, 10);
				JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());

				Dataset<AuthorData> authorDataset = spark
					.createDataset(
						sc
							.textFile(workingPath.concat(orcidDataFolder).concat("/authors/*"))
							.map(item -> OBJECT_MAPPER.readValue(item, AuthorSummary.class))
							.filter(authorSummary -> authorSummary.getAuthorData() != null)
							.map(AuthorSummary::getAuthorData)
							.rdd(),
						Encoders.bean(AuthorData.class));
				logger.info("Authors data loaded: {}", authorDataset.count());

				Dataset<WorkDetail> workDataset = spark
					.createDataset(
						sc
							.textFile(workingPath.concat(orcidDataFolder).concat("/works/*"))
							.map(item -> OBJECT_MAPPER.readValue(item, Work.class))
							.filter(work -> work.getWorkDetail() != null)
							.map(Work::getWorkDetail)
							.filter(work -> work.getErrorCode() == null)
							.filter(
								work -> work
									.getExtIds()
									.stream()
									.filter(e -> e.getType() != null)
									.noneMatch(e -> e.getType().equalsIgnoreCase("doi")))
							.rdd(),
						Encoders.bean(WorkDetail.class));
				logger.info("Works data loaded: {}", workDataset.count());

				final LongAccumulator warnNotFoundContributors = spark
					.sparkContext()
					.longAccumulator("warnNotFoundContributors");

				JavaRDD<Tuple2<String, String>> enrichedWorksRDD = workDataset
					.joinWith(
						authorDataset,
						workDataset.col("oid").equalTo(authorDataset.col("oid")), "inner")
					.map(
						(MapFunction<Tuple2<WorkDetail, AuthorData>, Tuple2<String, String>>) value -> {
							WorkDetail w = value._1;
							AuthorData a = value._2;
							if (w.getContributors() == null
								|| (w.getContributors() != null && w.getContributors().isEmpty())) {
								Contributor c = new Contributor();
								c.setName(a.getName());
								c.setSurname(a.getSurname());
								c.setCreditName(a.getCreditName());
								c.setOid(a.getOid());
								List<Contributor> contributors = Arrays.asList(c);
								w.setContributors(contributors);
								if (warnNotFoundContributors != null) {
									warnNotFoundContributors.add(1);
								}
							} else {
								AuthorMatcher.match(a, w.getContributors());
							}
							return new Tuple2<>(a.getOid(), JsonHelper.createOidWork(w));
						},
						Encoders.tuple(Encoders.STRING(), Encoders.STRING()))
					.filter(Objects::nonNull)
					.toJavaRDD();
				logger.info("Enriched works RDD ready.");

				final LongAccumulator parsedPublications = spark.sparkContext().longAccumulator("parsedPublications");
				final LongAccumulator enrichedPublications = spark
					.sparkContext()
					.longAccumulator("enrichedPublications");
				final LongAccumulator errorsGeneric = spark.sparkContext().longAccumulator("errorsGeneric");
				final LongAccumulator errorsInvalidTitle = spark.sparkContext().longAccumulator("errorsInvalidTitle");
				final LongAccumulator errorsNotFoundAuthors = spark
					.sparkContext()
					.longAccumulator("errorsNotFoundAuthors");
				final LongAccumulator errorsInvalidType = spark.sparkContext().longAccumulator("errorsInvalidType");
				final LongAccumulator otherTypeFound = spark.sparkContext().longAccumulator("otherTypeFound");
				final LongAccumulator deactivatedAcc = spark.sparkContext().longAccumulator("deactivated_found");
				final LongAccumulator titleNotProvidedAcc = spark
					.sparkContext()
					.longAccumulator("Title_not_provided_found");
				final LongAccumulator noUrlAcc = spark.sparkContext().longAccumulator("no_url_found");

				final PublicationToOaf publicationToOaf = new PublicationToOaf(
					parsedPublications,
					enrichedPublications,
					errorsInvalidTitle,
					errorsNotFoundAuthors,
					errorsInvalidType,
					otherTypeFound,
					deactivatedAcc,
					titleNotProvidedAcc,
					noUrlAcc,
					dateOfCollection);

				JavaRDD<Publication> oafPublicationRDD = enrichedWorksRDD
					.map(e -> (Publication) publicationToOaf.generatePublicationActionsFromJson(e._2()))
					.filter(Objects::nonNull);

				sc.hadoopConfiguration().set("mapreduce.output.fileoutputformat.compress", "true");

				oafPublicationRDD
					.mapToPair(
						p -> new Tuple2<>(p.getClass().toString(),
							OBJECT_MAPPER.writeValueAsString(new AtomicAction<>(Publication.class, p))))
					.mapToPair(t -> new Tuple2<>(new Text(t._1()), new Text(t._2())))
					.saveAsNewAPIHadoopFile(
						outputEnrichedWorksPath,
						Text.class,
						Text.class,
						SequenceFileOutputFormat.class,
						sc.hadoopConfiguration());

				logger.info("parsedPublications: {}", parsedPublications.value());
				logger.info("enrichedPublications: {}", enrichedPublications.value());
				logger.info("warnNotFoundContributors: {}", warnNotFoundContributors.value());
				logger.info("errorsGeneric: {}", errorsGeneric.value());
				logger.info("errorsInvalidTitle: {}", errorsInvalidTitle.value());
				logger.info("errorsNotFoundAuthors: {}", errorsNotFoundAuthors.value());
				logger.info("errorsInvalidType: {}", errorsInvalidType.value());
				logger.info("otherTypeFound: {}", otherTypeFound.value());
				logger.info("deactivatedAcc: {}", deactivatedAcc.value());
				logger.info("titleNotProvidedAcc: {}", titleNotProvidedAcc.value());
				logger.info("noUrlAcc: {}", noUrlAcc.value());
			});
	}
}
