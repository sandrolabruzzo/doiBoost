package eu.dnetlib.doiboost.crossref

import eu.dnetlib.dhp.application.ArgumentApplicationParser
import eu.dnetlib.dhp.schema.oaf
import eu.dnetlib.dhp.schema.oaf.{Oaf, Publication, Relation, Dataset => OafDataset}
import org.apache.commons.io.IOUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.slf4j.{Logger, LoggerFactory}

case class Reference(author: String, firstPage: String) {}

object SparkMapDumpIntoOAF {

  def main(args: Array[String]): Unit = {

    implicit val mrEncoder: Encoder[CrossrefDT] = Encoders.kryo[CrossrefDT]

    val logger: Logger = LoggerFactory.getLogger(SparkMapDumpIntoOAF.getClass)
    val conf: SparkConf = new SparkConf()
    val parser = new ArgumentApplicationParser(
      IOUtils.toString(
        SparkMapDumpIntoOAF.getClass.getResourceAsStream(
          "/eu/dnetlib/dhp/doiboost/convert_crossref_dump_to_oaf_params.json"
        )
      )
    )
    parser.parseArgument(args)
    val spark: SparkSession =
      SparkSession
        .builder()
        .config(conf)
        .appName(SparkMapDumpIntoOAF.getClass.getSimpleName)
        .master(parser.get("master"))
        .getOrCreate()

    implicit val oafEncoder: Encoder[Oaf] = Encoders.kryo[Oaf]
    implicit val mapEncoderPubs: Encoder[Publication] = Encoders.kryo[Publication]
    implicit val mapEncoderRelatons: Encoder[Relation] = Encoders.kryo[Relation]
    implicit val mapEncoderDatasets: Encoder[oaf.Dataset] = Encoders.kryo[OafDataset]

    val targetPath = parser.get("targetPath")

    spark.read
      .load(parser.get("sourcePath"))
      .as[CrossrefDT]
      .flatMap(k => Crossref2Oaf.convert(k.json))
      .filter(o => o != null)
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$targetPath/mixObject")

    val ds: Dataset[Oaf] = spark.read.load(s"$targetPath/mixObject").as[Oaf]

    ds.filter(o => o.isInstanceOf[Publication])
      .map(o => o.asInstanceOf[Publication])
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$targetPath/crossrefPublication")

    ds.filter(o => o.isInstanceOf[Relation])
      .map(o => o.asInstanceOf[Relation])
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$targetPath/crossrefRelation")

    ds.filter(o => o.isInstanceOf[OafDataset])
      .map(o => o.asInstanceOf[OafDataset])
      .write
      .mode(SaveMode.Overwrite)
      .save(s"$targetPath/crossrefDataset")
  }

}
