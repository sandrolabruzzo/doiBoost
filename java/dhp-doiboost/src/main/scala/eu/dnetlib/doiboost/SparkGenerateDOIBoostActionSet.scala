package eu.dnetlib.doiboost

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.action.AtomicAction
import eu.dnetlib.dhp.schema.oaf.{Organization, Publication, Relation, Dataset => OafDataset}
import org.apache.commons.io.IOUtils
import org.apache.hadoop.io.Text
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object SparkGenerateDOIBoostActionSet {
  val logger: Logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        getClass.getResourceAsStream("/eu/dnetlib/dhp/doiboost/generate_doiboost_as_params.json")
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

    implicit val mapEncoderPub: Encoder[Publication] = Encoders.kryo[Publication]
    implicit val mapEncoderOrg: Encoder[Organization] = Encoders.kryo[Organization]
    implicit val mapEncoderDataset: Encoder[OafDataset] = Encoders.kryo[OafDataset]
    implicit val mapEncoderRel: Encoder[Relation] = Encoders.kryo[Relation]
    implicit val mapEncoderAS: Encoder[(String, String)] =
      Encoders.tuple(Encoders.STRING, Encoders.STRING)

    implicit val mapEncoderAtomiAction: Encoder[AtomicAction[OafDataset]] =
      Encoders.kryo[AtomicAction[OafDataset]]

    val dbPublicationPath = parser.get("dbPublicationPath")
    val dbDatasetPath = parser.get("dbDatasetPath")
    val crossRefRelation = parser.get("crossRefRelation")
    val dbaffiliationRelationPath = parser.get("dbaffiliationRelationPath")
    val dbOrganizationPath = parser.get("dbOrganizationPath")
    val sequenceFilePath = parser.get("sFilePath")

    val asDataset = spark.read
      .load(dbDatasetPath)
      .as[OafDataset]
      .filter(p => p != null || p.getId != null)
      .map(d => DoiBoostMappingUtil.fixResult(d))
      .map(d => DoiBoostMappingUtil.toActionSet(d))(
        Encoders.tuple(Encoders.STRING, Encoders.STRING)
      )

    val asPublication = spark.read
      .load(dbPublicationPath)
      .as[Publication]
      .filter(p => p != null || p.getId != null)
      .map(d => DoiBoostMappingUtil.toActionSet(d))(
        Encoders.tuple(Encoders.STRING, Encoders.STRING)
      )

    val asOrganization = spark.read
      .load(dbOrganizationPath)
      .as[Organization]
      .map(d => DoiBoostMappingUtil.toActionSet(d))(
        Encoders.tuple(Encoders.STRING, Encoders.STRING)
      )

    val asCRelation = spark.read
      .load(crossRefRelation)
      .as[Relation]
      .filter(r => r != null && r.getSource != null && r.getTarget != null)
      .map(d => DoiBoostMappingUtil.toActionSet(d))(
        Encoders.tuple(Encoders.STRING, Encoders.STRING)
      )

    val asRelAffiliation = spark.read
      .load(dbaffiliationRelationPath)
      .as[Relation]
      .map(d => DoiBoostMappingUtil.toActionSet(d))(
        Encoders.tuple(Encoders.STRING, Encoders.STRING)
      )

    val d: Dataset[(String, String)] = asDataset
      .union(asPublication)
      .union(asOrganization)
      .union(asCRelation)
      .union(asRelAffiliation)

    d.rdd
      .repartition(6000)
      .map(s => (new Text(s._1), new Text(s._2)))
      .saveAsHadoopFile(
        s"$sequenceFilePath",
        classOf[Text],
        classOf[Text],
        classOf[SequenceFileOutputFormat[Text, Text]],
        classOf[GzipCodec]
      )

  }

}
