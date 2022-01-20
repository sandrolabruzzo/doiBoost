package eu.dnetlib.doiboost.mag

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf.Publication
import eu.dnetlib.doiboost.DoiBoostMappingUtil
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col, collect_list, struct}
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._

object SparkProcessMAG {

  def getDistinctResults(d: Dataset[MagPapers]): Dataset[MagPapers] = {
    d.where(col("Doi").isNotNull)
      .groupByKey(mp => DoiBoostMappingUtil.normalizeDoi(mp.Doi))(Encoders.STRING)
      .reduceGroups((p1: MagPapers, p2: MagPapers) => ConversionUtil.choiceLatestMagArtitcle(p1, p2))
      .map(_._2)(Encoders.product[MagPapers])
      .map(mp => {
        MagPapers(
          mp.PaperId,
          mp.Rank,
          DoiBoostMappingUtil.normalizeDoi(mp.Doi),
          mp.DocType,
          mp.PaperTitle,
          mp.OriginalTitle,
          mp.BookTitle,
          mp.Year,
          mp.Date,
          mp.Publisher: String,
          mp.JournalId,
          mp.ConferenceSeriesId,
          mp.ConferenceInstanceId,
          mp.Volume,
          mp.Issue,
          mp.FirstPage,
          mp.LastPage,
          mp.ReferenceCount,
          mp.CitationCount,
          mp.EstimatedCitation,
          mp.OriginalVenue,
          mp.FamilyId,
          mp.CreatedDate
        )
      })(Encoders.product[MagPapers])
  }

  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        getClass.getResourceAsStream("/eu/dnetlib/dhp/doiboost/mag/preprocess_mag_params.json")
      )
    )
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(getClass.getSimpleName)
        .master(parser.get("master"))
        .getOrCreate()

    val sourcePath = parser.get("sourcePath")
    val workingPath = parser.get("workingPath")
    val targetPath = parser.get("targetPath")

    import spark.implicits._
    implicit val mapEncoderPubs: Encoder[Publication] =
      org.apache.spark.sql.Encoders.kryo[Publication]
    implicit val tupleForJoinEncoder: Encoder[(String, Publication)] =
      Encoders.tuple(Encoders.STRING, mapEncoderPubs)

    logger.info("Phase 1) make uninue DOI in Papers:")
    val d: Dataset[MagPapers] = spark.read.load(s"$sourcePath/Papers").as[MagPapers]

    // Filtering Papers with DOI, and since for the same DOI we have multiple version of item with different PapersId we get the last one
    val distinctPaper: Dataset[MagPapers] = getDistinctResults(d)

    distinctPaper.write.mode(SaveMode.Overwrite).save(s"$workingPath/Papers_distinct")

    logger.info("Phase 0) Enrich Publication with description")
    val pa = spark.read.load(s"$sourcePath/PaperAbstractsInvertedIndex").as[MagPaperAbstract]
    pa.map(ConversionUtil.transformPaperAbstract)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingPath/PaperAbstract")

    logger.info("Phase 3) Group Author by PaperId")
    val authors = spark.read.load(s"$sourcePath/Authors").as[MagAuthor]

    val affiliation = spark.read.load(s"$sourcePath/Affiliations").as[MagAffiliation]
    val paperAuthorAffiliation =
      spark.read.load(s"$sourcePath/PaperAuthorAffiliations").as[MagPaperAuthorAffiliation]

    paperAuthorAffiliation
      .joinWith(authors, paperAuthorAffiliation("AuthorId").equalTo(authors("AuthorId")))
      .map { case (a: MagPaperAuthorAffiliation, b: MagAuthor) =>
        (a.AffiliationId, MagPaperAuthorDenormalized(a.PaperId, b, null, a.AuthorSequenceNumber))
      }
      .joinWith(affiliation, affiliation("AffiliationId").equalTo(col("_1")), "left")
      .map(s => {
        val mpa = s._1._2
        val af = s._2
        if (af != null) {
          MagPaperAuthorDenormalized(mpa.PaperId, mpa.author, af.DisplayName, mpa.sequenceNumber)
        } else
          mpa
      })
      .groupBy("PaperId")
      .agg(collect_list(struct($"author", $"affiliation", $"sequenceNumber")).as("authors"))
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingPath/merge_step_1_paper_authors")

    logger.info(
      "Phase 4) create First Version of publication Entity with Paper Journal and Authors"
    )

    val journals = spark.read.load(s"$sourcePath/Journals").as[MagJournal]

    val papers = spark.read.load(s"$workingPath/Papers_distinct").as[MagPapers]

    val paperWithAuthors =
      spark.read.load(s"$workingPath/merge_step_1_paper_authors").as[MagPaperWithAuthorList]

    val firstJoin =
      papers.joinWith(journals, papers("JournalId").equalTo(journals("JournalId")), "left")
    firstJoin
      .joinWith(
        paperWithAuthors,
        firstJoin("_1.PaperId").equalTo(paperWithAuthors("PaperId")),
        "left"
      )
      .map { a => ConversionUtil.createOAFFromJournalAuthorPaper(a) }
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingPath/merge_step_2")

    var magPubs: Dataset[(String, Publication)] =
      spark.read
        .load(s"$workingPath/merge_step_2")
        .as[Publication]
        .map(p => (ConversionUtil.extractMagIdentifier(p.getOriginalId.asScala), p))
        .as[(String, Publication)]

    val conference = spark.read
      .load(s"$sourcePath/ConferenceInstances")
      .select(
        $"ConferenceInstanceId".as("ci"),
        $"DisplayName",
        $"Location",
        $"StartDate",
        $"EndDate"
      )
    val conferenceInstance = conference
      .joinWith(papers, papers("ConferenceInstanceId").equalTo(conference("ci")))
      .select(
        $"_1.ci",
        $"_1.DisplayName",
        $"_1.Location",
        $"_1.StartDate",
        $"_1.EndDate",
        $"_2.PaperId"
      )
      .as[MagConferenceInstance]

    magPubs
      .joinWith(conferenceInstance, col("_1").equalTo(conferenceInstance("PaperId")), "left")
      .map(item => ConversionUtil.updatePubsWithConferenceInfo(item))
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingPath/merge_step_3")

    val paperAbstract = spark.read.load(s"$workingPath/PaperAbstract").as[MagPaperAbstract]

    magPubs = spark.read
      .load(s"$workingPath/merge_step_3")
      .as[Publication]
      .map(p => (ConversionUtil.extractMagIdentifier(p.getOriginalId.asScala), p))
      .as[(String, Publication)]

    magPubs
      .joinWith(paperAbstract, col("_1").equalTo(paperAbstract("PaperId")), "left")
      .map(item => ConversionUtil.updatePubsWithDescription(item))
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingPath/merge_step_4")

    logger.info("Phase 7) Enrich Publication with FieldOfStudy")

    magPubs = spark.read
      .load(s"$workingPath/merge_step_4")
      .as[Publication]
      .map(p => (ConversionUtil.extractMagIdentifier(p.getOriginalId.asScala), p))
      .as[(String, Publication)]

    val fos = spark.read
      .load(s"$sourcePath/FieldsOfStudy")
      .select($"FieldOfStudyId".alias("fos"), $"DisplayName", $"MainType")

    val pfos = spark.read.load(s"$sourcePath/PaperFieldsOfStudy")

    val paperField = pfos
      .joinWith(fos, fos("fos").equalTo(pfos("FieldOfStudyId")))
      .select($"_1.FieldOfStudyId", $"_2.DisplayName", $"_2.MainType", $"_1.PaperId", $"_1.Score")
      .groupBy($"PaperId")
      .agg(
        collect_list(struct($"FieldOfStudyId", $"DisplayName", $"MainType", $"Score"))
          .as("subjects")
      )
      .as[MagFieldOfStudy]

    magPubs
      .joinWith(
        paperField,
        col("_1")
          .equalTo(paperField("PaperId")),
        "left"
      )
      .map(item => ConversionUtil.updatePubsWithSubject(item))
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingPath/mag_publication")

    spark.read
      .load(s"$workingPath/mag_publication")
      .as[Publication]
      .filter(p => p.getId != null)
      .groupByKey(p => p.getId)
      .reduceGroups((a: Publication, b: Publication) => ConversionUtil.mergePublication(a, b))
      .map(_._2)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$targetPath/magPublication")

  }
}
