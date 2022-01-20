package eu.dnetlib.doiboost

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.oa.merge.AuthorMerger
import eu.dnetlib.dhp.schema.common.ModelConstants
import eu.dnetlib.dhp.schema.oaf.{Organization, Publication, Relation, Dataset => OafDataset}
import eu.dnetlib.doiboost.mag.ConversionUtil
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.functions.col
import org.apache.spark.sql._
import org.json4s.DefaultFormats
import org.json4s.JsonAST.{JField, JObject, JString}
import org.json4s.jackson.JsonMethods.parse
import org.slf4j.{Logger, LoggerFactory}
import scala.collection.JavaConverters._

object SparkGenerateDoiBoost {

  def extractIdGRID(input: String): List[(String, String)] = {
    implicit lazy val formats: DefaultFormats.type = org.json4s.DefaultFormats
    lazy val json: org.json4s.JValue = parse(input)

    val id: String = (json \ "id").extract[String]

    val grids: List[String] = for {

      JObject(pid)                            <- json \ "pid"
      JField("qualifier", JObject(qualifier)) <- pid
      JField("classid", JString(classid))     <- qualifier
      JField("value", JString(vl))            <- pid
      if classid == "GRID"
    } yield vl
    grids.map(g => (id, s"unresolved::grid::${g.toLowerCase}"))(collection.breakOut)
  }

  def main(args: Array[String]): Unit = {

    val logger: Logger = LoggerFactory.getLogger(getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        getClass.getResourceAsStream("/eu/dnetlib/dhp/doiboost/generate_doiboost_params.json")
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

    import spark.implicits._

    val hostedByMapPath = parser.get("hostedByMapPath")
    val workingDirPath = parser.get("workingPath")
    val openaireOrganizationPath = parser.get("openaireOrganizationPath")

    val crossrefAggregator = new Aggregator[(String, Publication), Publication, Publication] with Serializable {
      override def zero: Publication = new Publication

      override def reduce(b: Publication, a: (String, Publication)): Publication = {

        if (b == null) {
          if (a != null && a._2 != null) {
            a._2.setId(a._1)
            return a._2
          }
        } else {
          if (a != null && a._2 != null) {
            b.mergeFrom(a._2)
            b.setId(a._1)
            val authors = AuthorMerger.mergeAuthor(b.getAuthor, a._2.getAuthor)
            b.setAuthor(authors)
            return b
          }
        }
        new Publication
      }

      override def merge(b1: Publication, b2: Publication): Publication = {
        if (b1 == null) {
          if (b2 != null)
            return b2
        } else {
          if (b2 != null) {
            b1.mergeFrom(b2)
            val authors = AuthorMerger.mergeAuthor(b1.getAuthor, b2.getAuthor)
            b1.setAuthor(authors)
            if (b2.getId != null && b2.getId.nonEmpty)
              b1.setId(b2.getId)
            return b1
          }
        }
        new Publication
      }

      override def finish(reduction: Publication): Publication = reduction

      override def bufferEncoder: Encoder[Publication] = Encoders.kryo[Publication]

      override def outputEncoder: Encoder[Publication] = Encoders.kryo[Publication]
    }

    implicit val mapEncoderPub: Encoder[Publication] = Encoders.kryo[Publication]
    implicit val mapEncoderOrg: Encoder[Organization] = Encoders.kryo[Organization]
    implicit val mapEncoderDataset: Encoder[OafDataset] = Encoders.kryo[OafDataset]
    implicit val tupleForJoinEncoder: Encoder[(String, Publication)] =
      Encoders.tuple(Encoders.STRING, mapEncoderPub)
    implicit val mapEncoderRel: Encoder[Relation] = Encoders.kryo[Relation]

    logger.info("Phase 2) Join Crossref with UnpayWall")

    val crossrefPublication: Dataset[(String, Publication)] =
      spark.read.load(s"$workingDirPath/crossrefPublication").as[Publication].map(p => (p.getId, p))
    val uwPublication: Dataset[(String, Publication)] =
      spark.read.load(s"$workingDirPath/uwPublication").as[Publication].map(p => (p.getId, p))

    def applyMerge(item: ((String, Publication), (String, Publication))): Publication = {
      val crossrefPub = item._1._2
      if (item._2 != null) {
        val otherPub = item._2._2
        if (otherPub != null) {
          crossrefPub.mergeFrom(otherPub)
          crossrefPub.setAuthor(AuthorMerger.mergeAuthor(crossrefPub.getAuthor, otherPub.getAuthor))
        }
      }
      crossrefPub
    }

    crossrefPublication
      .joinWith(uwPublication, crossrefPublication("_1").equalTo(uwPublication("_1")), "left")
      .map(applyMerge)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingDirPath/firstJoin")
    logger.info("Phase 3) Join Result with ORCID")
    val fj: Dataset[(String, Publication)] =
      spark.read.load(s"$workingDirPath/firstJoin").as[Publication].map(p => (p.getId, p))
    val orcidPublication: Dataset[(String, Publication)] =
      spark.read.load(s"$workingDirPath/orcidPublication").as[Publication].map(p => (p.getId, p))
    fj.joinWith(orcidPublication, fj("_1").equalTo(orcidPublication("_1")), "left")
      .map(applyMerge)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingDirPath/secondJoin")

    logger.info("Phase 4) Join Result with MAG")
    val sj: Dataset[(String, Publication)] =
      spark.read.load(s"$workingDirPath/secondJoin").as[Publication].map(p => (p.getId, p))

    val magPublication: Dataset[(String, Publication)] =
      spark.read.load(s"$workingDirPath/magPublication").as[Publication].map(p => (p.getId, p))
    sj.joinWith(magPublication, sj("_1").equalTo(magPublication("_1")), "left")
      .map(applyMerge)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingDirPath/doiBoostPublication")

    val doiBoostPublication: Dataset[(String, Publication)] = spark.read
      .load(s"$workingDirPath/doiBoostPublication")
      .as[Publication]
      .filter(p => DoiBoostMappingUtil.filterPublication(p))
      .map(DoiBoostMappingUtil.toISSNPair)(tupleForJoinEncoder)

    val hostedByDataset: Dataset[(String, HostedByItemType)] = spark.createDataset(
      spark.sparkContext.textFile(hostedByMapPath).map(DoiBoostMappingUtil.toHostedByItem)
    )

    doiBoostPublication
      .joinWith(hostedByDataset, doiBoostPublication("_1").equalTo(hostedByDataset("_1")), "left")
      .map(DoiBoostMappingUtil.fixPublication)
      .map(p => (p.getId, p))
      .groupByKey(_._1)
      .agg(crossrefAggregator.toColumn)
      .map(p => p._2)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingDirPath/doiBoostPublicationFiltered")

    val affiliationPath = parser.get("affiliationPath")
    val paperAffiliationPath = parser.get("paperAffiliationPath")

    val affiliation = spark.read
      .load(affiliationPath)
      .select(col("AffiliationId"), col("GridId"), col("OfficialPage"), col("DisplayName"))

    val paperAffiliation = spark.read
      .load(paperAffiliationPath)
      .select(col("AffiliationId").alias("affId"), col("PaperId"))

    val a: Dataset[DoiBoostAffiliation] = paperAffiliation
      .joinWith(affiliation, paperAffiliation("affId").equalTo(affiliation("AffiliationId")))
      .select(
        col("_1.PaperId"),
        col("_2.AffiliationId"),
        col("_2.GridId"),
        col("_2.OfficialPage"),
        col("_2.DisplayName")
      )
      .as[DoiBoostAffiliation]

    val magPubs: Dataset[(String, Publication)] = spark.read
      .load(s"$workingDirPath/doiBoostPublicationFiltered")
      .as[Publication]
      .map(p => (ConversionUtil.extractMagIdentifier(p.getOriginalId.asScala), p))(
        tupleForJoinEncoder
      )
      .filter(s => s._1 != null)

    magPubs
      .joinWith(a, magPubs("_1").equalTo(a("PaperId")))
      .flatMap(item => {
        val pub: Publication = item._1._2
        val affiliation = item._2
        val affId: String =
          if (affiliation.GridId.isDefined)
            s"unresolved::grid::${affiliation.GridId.get.toLowerCase}"
          else DoiBoostMappingUtil.generateMAGAffiliationId(affiliation.AffiliationId.toString)
        val r: Relation = new Relation
        r.setSource(pub.getId)
        r.setTarget(affId)
        r.setRelType(ModelConstants.RESULT_ORGANIZATION)
        r.setRelClass(ModelConstants.HAS_AUTHOR_INSTITUTION)
        r.setSubRelType(ModelConstants.AFFILIATION)
        r.setDataInfo(pub.getDataInfo)
        r.setCollectedfrom(List(DoiBoostMappingUtil.createMAGCollectedFrom()).asJava)
        val r1: Relation = new Relation
        r1.setTarget(pub.getId)
        r1.setSource(affId)
        r1.setRelType(ModelConstants.RESULT_ORGANIZATION)
        r1.setRelClass(ModelConstants.IS_AUTHOR_INSTITUTION_OF)
        r1.setSubRelType(ModelConstants.AFFILIATION)
        r1.setDataInfo(pub.getDataInfo)
        r1.setCollectedfrom(List(DoiBoostMappingUtil.createMAGCollectedFrom()).asJava)
        List(r, r1)
      })(mapEncoderRel)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingDirPath/doiBoostPublicationAffiliation_unresolved")

    val unresolvedRels: Dataset[(String, Relation)] = spark.read
      .load(s"$workingDirPath/doiBoostPublicationAffiliation_unresolved")
      .as[Relation]
      .map(r => {

        if (r.getSource.startsWith("unresolved"))
          (r.getSource, r)
        else if (r.getTarget.startsWith("unresolved"))
          (r.getTarget, r)
        else
          ("resolved", r)
      })(Encoders.tuple(Encoders.STRING, mapEncoderRel))

    val openaireOrganization: Dataset[(String, String)] = spark.read
      .text(openaireOrganizationPath)
      .as[String]
      .flatMap(s => extractIdGRID(s))
      .groupByKey(_._2)
      .reduceGroups((x, y) => if (x != null) x else y)
      .map(_._2)

    unresolvedRels
      .joinWith(openaireOrganization, unresolvedRels("_1").equalTo(openaireOrganization("_2")))
      .map { x =>
        val currentRels = x._1._2
        val currentOrgs = x._2
        if (currentOrgs != null)
          if (currentRels.getSource.startsWith("unresolved"))
            currentRels.setSource(currentOrgs._1)
          else
            currentRels.setTarget(currentOrgs._1)
        currentRels
      }
      .filter(r => !r.getSource.startsWith("unresolved") && !r.getTarget.startsWith("unresolved"))
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingDirPath/doiBoostPublicationAffiliation")

    magPubs
      .joinWith(a, magPubs("_1").equalTo(a("PaperId")))
      .map(item => {
        val affiliation = item._2
        if (affiliation.GridId.isEmpty) {
          val o = new Organization
          o.setCollectedfrom(List(DoiBoostMappingUtil.createMAGCollectedFrom()).asJava)
          o.setDataInfo(DoiBoostMappingUtil.generateDataInfo())
          o.setId(DoiBoostMappingUtil.generateMAGAffiliationId(affiliation.AffiliationId.toString))
          o.setOriginalId(List(affiliation.AffiliationId.toString).asJava)
          if (affiliation.DisplayName.nonEmpty)
            o.setLegalname(DoiBoostMappingUtil.asField(affiliation.DisplayName.get))
          if (affiliation.OfficialPage.isDefined)
            o.setWebsiteurl(DoiBoostMappingUtil.asField(affiliation.OfficialPage.get))
          o.setCountry(ModelConstants.UNKNOWN_COUNTRY)
          o
        } else
          null
      })
      .filter(o => o != null)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$workingDirPath/doiBoostOrganization")
  }

}
